package dispatch

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
)

type PublishErrorCode string

const (
	// PublishNotFound: unknown plan ID.
	PublishNotFound PublishErrorCode = "not_found"
	// PublishConflict: plan is in a state that cannot be published (already
	// published, still calculating under a stale revision, calculation
	// failed). Caller should query the plan rather than retry blindly.
	PublishConflict PublishErrorCode = "conflict"
	// PublishRejected: the atomic target commit refused the plan (manual hold,
	// temperature outside envelope, unknown station). Nothing was applied;
	// fixing the cause and retrying is safe.
	PublishRejected PublishErrorCode = "rejected"
	// PublishAuditUnavailable: targets and plan state are settled, but the
	// audit trail could not be written. Querying the plan returns the real
	// result and retrying Publish backfills the audit without side effects.
	PublishAuditUnavailable PublishErrorCode = "audit_unavailable"
)

// PublishError carries the failure classification together with the plan as it
// really was when the failure happened, so callers can inspect partial results
// instead of guessing from the error text.
type PublishError struct {
	Code PublishErrorCode
	Plan Plan
	err  error
}

func (e *PublishError) Error() string { return e.err.Error() }
func (e *PublishError) Unwrap() error { return e.err }

type Publisher struct {
	planner *Planner
	targets *station.TargetService
	audit   *audit.Recorder
}

func NewPublisher(planner *Planner, targets *station.TargetService, recorder *audit.Recorder) *Publisher {
	return &Publisher{planner: planner, targets: targets, audit: recorder}
}

// Publish commits a calculated plan's targets to the stations.
//
// The operation is idempotent and crash-recoverable:
//
//   - Reservation of the plan (calculating -> publishing) is a compare-and-set,
//     so concurrent publish calls cannot double-commit; the loser replays.
//   - The target commit is atomic (validate everything first, then apply under
//     one revision) and idempotent on the plan ID, so a retry after a crash
//     between the commit and the status update never applies targets twice.
//   - The plan is marked published together with the committed revision and the
//     values actually applied; those fields let a caller query the real partial
//     result after any failure.
//   - Audit entries use deterministic IDs and are backfilled on retry, so an
//     audit outage after a successful commit neither loses the result nor
//     produces duplicates.
func (p *Publisher) Publish(planID, actor string) (Plan, error) {
	plan, ok := p.planner.Plan(planID)
	if !ok {
		return Plan{}, &PublishError{Code: PublishNotFound, err: fmt.Errorf("plan %s not found", planID)}
	}

	switch plan.Status {
	case PlanPublished:
		// Already committed: only reconcile the audit trail if a previous try
		// died before writing it. No targets are touched.
		if err := p.recordAccepted(plan, actor); err != nil {
			return plan, &PublishError{Code: PublishAuditUnavailable, Plan: plan, err: err}
		}
		return plan, nil
	case PlanStale:
		if err := p.recordRejected(plan, actor, plan.Message); err != nil {
			return plan, &PublishError{Code: PublishAuditUnavailable, Plan: plan, err: err}
		}
		return plan, &PublishError{Code: PublishConflict, Plan: plan, err: errors.New(plan.Message)}
	case PlanFailed:
		return plan, &PublishError{Code: PublishConflict, Plan: plan, err: fmt.Errorf("plan %s failed during calculation: %s", plan.ID, plan.Message)}
	case PlanPublishing, PlanPublishFailed:
		// Resume a previous attempt below.
	case PlanCalculating:
		reserved, err := p.planner.Transition(planID, []PlanStatus{PlanCalculating}, func(pl *Plan) {
			pl.Status = PlanPublishing
			pl.Message = ""
		})
		if err != nil {
			var stateErr *PlanStateError
			if errors.As(err, &stateErr) {
				// A concurrent caller reserved it first: replay its attempt.
				return p.replay(stateErr.Plan, actor)
			}
			return plan, &PublishError{Code: PublishConflict, Plan: plan, err: err}
		}
		plan = reserved
	default:
		return plan, &PublishError{Code: PublishConflict, Plan: plan, err: fmt.Errorf("plan %s is %s", plan.ID, plan.Status)}
	}

	return p.commit(plan, actor)
}

// replay re-enters Publish with a plan read by a competing transition.
func (p *Publisher) replay(plan Plan, actor string) (Plan, error) {
	switch plan.Status {
	case PlanPublished:
		if err := p.recordAccepted(plan, actor); err != nil {
			return plan, &PublishError{Code: PublishAuditUnavailable, Plan: plan, err: err}
		}
		return plan, nil
	case PlanPublishing, PlanPublishFailed:
		return p.commit(plan, actor)
	default:
		// Re-read authoritative state and recurse once through the top switch.
		fresh, ok := p.planner.Plan(plan.ID)
		if !ok {
			return Plan{}, &PublishError{Code: PublishNotFound, err: fmt.Errorf("plan %s not found", plan.ID)}
		}
		return p.Publish(fresh.ID, actor)
	}
}

// commit performs the idempotent target commit for a plan in publishing.
func (p *Publisher) commit(plan Plan, actor string) (Plan, error) {
	result, err := p.targets.CompareAndPublish(plan.ID, plan.SourceRevision, plan.Targets)
	if err != nil {
		return p.failCommit(plan, actor, err)
	}

	now := time.Now().UTC()
	finalized, transErr := p.planner.Transition(plan.ID, []PlanStatus{PlanPublishing, PlanPublishFailed}, func(pl *Plan) {
		pl.Status = PlanPublished
		pl.CommittedRevision = result.Revision
		pl.AppliedTargets = cloneTargets(result.Applied)
		pl.PublishedAt = now
		pl.Message = ""
	})
	if transErr != nil {
		var stateErr *PlanStateError
		if errors.As(transErr, &stateErr) && stateErr.Plan.Status == PlanPublished {
			plan = stateErr.Plan
		} else {
			// Targets are committed under result.Revision even though the plan
			// record lags; surface the real partial result.
			return plan, &PublishError{Code: PublishAuditUnavailable, Plan: plan,
				err: fmt.Errorf("commit applied at revision %d but plan status could not be finalized: %w", result.Revision, transErr)}
		}
	} else {
		plan = finalized
	}

	if err := p.recordAccepted(plan, actor); err != nil {
		return plan, &PublishError{Code: PublishAuditUnavailable, Plan: plan, err: err}
	}
	return plan, nil
}

// failCommit records why the atomic commit refused and settles the plan into a
// retryable or terminal state. Nothing was applied to any station here.
func (p *Publisher) failCommit(plan Plan, actor string, cause error) (Plan, error) {
	if errors.Is(cause, station.ErrStaleRevision) {
		updated, err := p.planner.Transition(plan.ID, []PlanStatus{PlanPublishing, PlanPublishFailed}, func(pl *Plan) {
			pl.Status = PlanStale
			pl.Message = cause.Error()
		})
		if err == nil {
			plan = updated
		}
		if auditErr := p.recordRejected(plan, actor, cause.Error()); auditErr != nil {
			return plan, &PublishError{Code: PublishAuditUnavailable, Plan: plan, err: auditErr}
		}
		return plan, &PublishError{Code: PublishConflict, Plan: plan, err: cause}
	}

	updated, err := p.planner.Transition(plan.ID, []PlanStatus{PlanPublishing, PlanPublishFailed}, func(pl *Plan) {
		pl.Status = PlanPublishFailed
		pl.Message = cause.Error()
	})
	if err == nil {
		plan = updated
	}
	if auditErr := p.recordRejected(plan, actor, cause.Error()); auditErr != nil {
		return plan, &PublishError{Code: PublishAuditUnavailable, Plan: plan, err: auditErr}
	}
	return plan, &PublishError{Code: PublishRejected, Plan: plan, err: cause}
}

func (p *Publisher) recordAccepted(plan Plan, actor string) error {
	_, _, err := p.audit.RecordOnce(auditEntryID(plan.ID, "accepted"), audit.Entry{
		Action:   "dispatch.publish",
		EntityID: plan.ID,
		Actor:    actor,
		Outcome:  audit.OutcomeAccepted,
		Message:  "dispatch targets published",
		Details: map[string]any{
			"source_revision":    plan.SourceRevision,
			"committed_revision": plan.CommittedRevision,
			"applied":            plan.AppliedTargets,
		},
	})
	return err
}

func (p *Publisher) recordRejected(plan Plan, actor, reason string) error {
	_, _, err := p.audit.RecordOnce(auditEntryID(plan.ID, "rejected"), audit.Entry{
		Action:   "dispatch.publish",
		EntityID: plan.ID,
		Actor:    actor,
		Outcome:  audit.OutcomeRejected,
		Message:  reason,
		Details: map[string]any{
			"source_revision": plan.SourceRevision,
		},
	})
	return err
}

// auditEntryID derives a stable audit entry ID per (plan, outcome) so retries
// backfill instead of duplicating audit entries.
func auditEntryID(planID, outcome string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("heatpulse:dispatch.publish:"+planID+":"+outcome)).String()
}

func cloneTargets(targets map[string]float64) map[string]float64 {
	copy := make(map[string]float64, len(targets))
	for stationID, value := range targets {
		copy[stationID] = value
	}
	return copy
}
