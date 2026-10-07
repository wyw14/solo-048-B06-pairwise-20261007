package dispatch

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
)

// ErrPlanNotFound is returned when a publish targets an unknown plan.
var ErrPlanNotFound = errors.New("plan not found")

type Publisher struct {
	planner *Planner
	targets *station.TargetService
	audit   *audit.Recorder
	// mu serializes the check-then-act of Publish so concurrent calls for
	// the same plan cannot both attempt the publish and clobber each
	// other's outcome.
	mu sync.Mutex
}

func NewPublisher(planner *Planner, targets *station.TargetService, recorder *audit.Recorder) *Publisher {
	return &Publisher{planner: planner, targets: targets, audit: recorder}
}

// Publish applies a calculated plan to the station targets and records the
// outcome. It is safe to call again after a failure or timeout: an already
// published plan is returned as-is, a terminally rejected plan reports its
// stored outcome, and only a plan still in calculating state is attempted.
// The returned plan always reflects the persisted state, so callers can
// inspect exactly what happened.
func (p *Publisher) Publish(planID, actor string) (Plan, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	plan, ok := p.planner.Plan(planID)
	if !ok {
		return Plan{}, fmt.Errorf("%w: %s", ErrPlanNotFound, planID)
	}

	if plan.Status != PlanCalculating {
		// The plan already reached a terminal state. Only the audit record
		// may still be missing (e.g. the log was unavailable last time);
		// backfill it before reporting the stored outcome.
		p.backfillAudit(&plan, actor)
		if plan.Status == PlanPublished {
			return plan, nil
		}
		return plan, fmt.Errorf("plan %s is %s", plan.ID, plan.Status)
	}

	appliedRevision, err := p.targets.CompareAndPublish(plan.SourceRevision, plan.Targets)
	if err != nil {
		// CompareAndPublish is atomic, so no station target changed and the
		// revision is untouched. Classify the failure honestly: a stale
		// source revision calls for recalculation, anything else is a plan
		// that can never be applied as-is.
		var stale *station.StaleError
		if errors.As(err, &stale) {
			plan.Status = PlanStale
		} else {
			plan.Status = PlanFailed
		}
		plan.Message = err.Error()
		p.recordOutcome(&plan, actor, audit.OutcomeRejected, plan.Message, nil)
		return plan, err
	}

	plan.Status = PlanPublished
	plan.PublishedAt = time.Now().UTC()
	plan.AppliedRevision = appliedRevision
	p.recordOutcome(&plan, actor, audit.OutcomeAccepted, "dispatch targets published", map[string]any{
		"applied_revision": appliedRevision,
	})
	return plan, nil
}

// recordOutcome persists the plan's terminal state and writes the matching
// audit record. An audit failure never changes the outcome and is never
// reported as a publish failure: the state change already happened, so the
// gap is kept on the plan (AuditError set, AuditedAt zero) where it stays
// queryable, and a later Publish retry backfills the record.
func (p *Publisher) recordOutcome(plan *Plan, actor string, outcome audit.Outcome, message string, details map[string]any) {
	if details == nil {
		details = map[string]any{}
	}
	details["source_revision"] = plan.SourceRevision
	var err error
	if outcome == audit.OutcomeAccepted {
		err = p.audit.Accepted("dispatch.publish", plan.ID, actor, message, details)
	} else {
		err = p.audit.Rejected("dispatch.publish", plan.ID, actor, message, details)
	}
	if err != nil {
		plan.AuditError = fmt.Sprintf("audit record pending: %v", err)
	} else {
		plan.AuditedAt = time.Now().UTC()
		plan.AuditError = ""
	}
	p.planner.save(*plan)
}

// backfillAudit writes the terminal audit record for a plan whose outcome is
// already decided, if a previous attempt left it unrecorded.
func (p *Publisher) backfillAudit(plan *Plan, actor string) {
	if !plan.AuditedAt.IsZero() {
		return
	}
	if plan.Status == PlanPublished {
		p.recordOutcome(plan, actor, audit.OutcomeAccepted, "dispatch targets published", map[string]any{
			"applied_revision": plan.AppliedRevision,
		})
		return
	}
	p.recordOutcome(plan, actor, audit.OutcomeRejected, plan.Message, nil)
}
