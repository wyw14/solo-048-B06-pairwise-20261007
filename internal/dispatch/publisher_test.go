package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
)

// flakyEventStore is an in-memory event store that fails the first failUntil
// appends, simulating an audit outage during a publish.
type flakyEventStore struct {
	mu        sync.Mutex
	events    []store.Event
	failUntil int
	attempts  int
}

func (s *flakyEventStore) Append(event store.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.attempts <= s.failUntil {
		return fmt.Errorf("audit log unavailable (attempt %d)", s.attempts)
	}
	for _, existing := range s.events {
		if existing.ID == event.ID {
			return store.ErrEventExists
		}
	}
	s.events = append(s.events, event)
	return nil
}

func (s *flakyEventStore) Contains(id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.events {
		if event.ID == id {
			return true, nil
		}
	}
	return false, nil
}

func (s *flakyEventStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

func (s *flakyEventStore) outcomes() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := map[string]int{}
	for _, event := range s.events {
		counts[string(event.Fields["outcome"].(audit.Outcome))]++
	}
	return counts
}

func newHarness(t *testing.T, failUntil int) (*station.Registry, *station.TargetService, *Planner, *Publisher, *flakyEventStore) {
	t.Helper()
	registry := station.NewRegistry([]station.Station{
		{ID: "s1", TargetCelsius: 60, TargetRevision: 1},
		{ID: "s2", TargetCelsius: 62, TargetRevision: 1},
	})
	targets := station.NewTargetService(registry)
	planner := NewPlanner(targets)
	log := &flakyEventStore{failUntil: failUntil}
	recorder := audit.NewRecorder(log)
	publisher := NewPublisher(planner, targets, recorder)
	return registry, targets, planner, publisher, log
}

func beginPlan(t *testing.T, planner *Planner, values map[string]float64) Plan {
	t.Helper()
	plan, err := planner.Begin(context.Background(), values, 0)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return plan
}

func TestPublishHappyPath(t *testing.T) {
	registry, targets, planner, publisher, log := newHarness(t, 0)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70, "s2": 71})

	published, err := publisher.Publish(plan.ID, "alice")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if published.Status != PlanPublished || published.CommittedRevision != 2 {
		t.Fatalf("published plan = %+v", published)
	}
	if published.AppliedTargets["s1"] != 70 || published.AppliedTargets["s2"] != 71 {
		t.Fatalf("applied targets = %v", published.AppliedTargets)
	}
	if targets.Revision() != 2 {
		t.Fatalf("revision = %d, want 2", targets.Revision())
	}
	item, _ := registry.Get("s1")
	if item.TargetCelsius != 70 || item.TargetRevision != 2 {
		t.Fatalf("s1 = %+v", item)
	}
	if got := log.outcomes(); got["accepted"] != 1 {
		t.Fatalf("audit outcomes = %v, want exactly 1 accepted", got)
	}
}

func TestPublishRetriesAuditBackfillAfterOutage(t *testing.T) {
	_, _, planner, publisher, log := newHarness(t, 1)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70})

	// First attempt: targets commit, but the accepted audit entry fails.
	_, err := publisher.Publish(plan.ID, "alice")
	if err == nil {
		t.Fatal("expected audit unavailable error")
	}
	var publishErr *PublishError
	if !errors.As(err, &publishErr) || publishErr.Code != PublishAuditUnavailable {
		t.Fatalf("err = %v, want audit_unavailable", err)
	}
	// Targets are already live and the plan is published despite the error.
	if publishErr.Plan.Status != PlanPublished || publishErr.Plan.CommittedRevision != 2 {
		t.Fatalf("plan after audit failure = %+v", publishErr.Plan)
	}

	// Retry must not re-apply targets and must backfill the single audit entry.
	retry, err := publisher.Publish(plan.ID, "alice")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.Status != PlanPublished {
		t.Fatalf("retry status = %s", retry.Status)
	}
	if got := log.outcomes(); got["accepted"] != 1 || got["rejected"] != 0 {
		t.Fatalf("audit outcomes after retry = %v", got)
	}
}

func TestPublishConcurrentCommitsExactlyOnce(t *testing.T) {
	_, targets, planner, publisher, log := newHarness(t, 0)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70, "s2": 71})

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = publisher.Publish(plan.ID, "alice")
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent publish error: %v", err)
		}
	}
	if targets.Revision() != 2 {
		t.Fatalf("revision = %d, want exactly one commit (2)", targets.Revision())
	}
	if got := log.outcomes(); got["accepted"] != 1 {
		t.Fatalf("accepted audit entries = %v, want 1", got)
	}
}

func TestPublishStalePlanDoesNotApplyAndRetryReportsConflict(t *testing.T) {
	registry, targets, planner, publisher, log := newHarness(t, 0)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70})

	// Someone else advances the revision after the plan was calculated.
	if _, err := targets.ManualHold([]string{"s2"}, "bob"); err != nil {
		t.Fatalf("hold: %v", err)
	}
	_, err := publisher.Publish(plan.ID, "alice")
	if err == nil {
		t.Fatal("expected conflict")
	}
	var publishErr *PublishError
	if !errors.As(err, &publishErr) || publishErr.Code != PublishConflict {
		t.Fatalf("err = %v, want conflict", err)
	}
	stored, _ := planner.Plan(plan.ID)
	if stored.Status != PlanStale {
		t.Fatalf("plan status = %s, want stale", stored.Status)
	}
	item, _ := registry.Get("s1")
	if item.TargetCelsius != 60 {
		t.Fatalf("s1 applied despite stale revision: %+v", item)
	}

	// Retrying the stale plan keeps reporting conflict and never applies.
	_, err = publisher.Publish(plan.ID, "alice")
	if err == nil {
		t.Fatal("retry of stale plan must conflict")
	}
	item, _ = registry.Get("s1")
	if item.TargetCelsius != 60 {
		t.Fatalf("s1 applied on stale retry: %+v", item)
	}
	if got := log.outcomes(); got["rejected"] != 1 {
		t.Fatalf("rejected audit entries = %v, want 1", got)
	}
}

func TestPublishRejectedIsSafeToRetryAfterCauseIsFixed(t *testing.T) {
	registry, targets, planner, publisher, log := newHarness(t, 0)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70, "s2": 71})

	// A station becomes held without the target service revision moving
	// (e.g. the hold bookkeeping is reconciled out of band). The atomic commit
	// must refuse everything and apply nothing.
	if _, err := registry.Update("s2", func(item *station.Station) error {
		item.Hold = true
		return nil
	}); err != nil {
		t.Fatalf("hold: %v", err)
	}
	_, err := publisher.Publish(plan.ID, "alice")
	var publishErr *PublishError
	if !errors.As(err, &publishErr) || publishErr.Code != PublishRejected {
		t.Fatalf("err = %v, want rejected", err)
	}
	stored, _ := planner.Plan(plan.ID)
	if stored.Status != PlanPublishFailed {
		t.Fatalf("status = %s, want publish_failed", stored.Status)
	}
	if targets.Revision() != 1 {
		t.Fatalf("revision = %d, want unchanged 1", targets.Revision())
	}
	item, _ := registry.Get("s1")
	if item.TargetCelsius != 60 {
		t.Fatalf("s1 applied despite sibling hold: %+v", item)
	}

	// Fix the cause and retry the same plan: it commits exactly once.
	if _, err := registry.Update("s2", func(item *station.Station) error {
		item.Hold = false
		return nil
	}); err != nil {
		t.Fatalf("release: %v", err)
	}
	retry, err := publisher.Publish(plan.ID, "alice")
	if err != nil {
		t.Fatalf("retry after fix: %v", err)
	}
	if retry.Status != PlanPublished {
		t.Fatalf("retry status = %s", retry.Status)
	}
	item, _ = registry.Get("s1")
	if item.TargetCelsius != 70 {
		t.Fatalf("s1 = %v, want 70 after retry", item.TargetCelsius)
	}
	if targets.Revision() != 2 {
		t.Fatalf("revision = %d, want exactly one commit (2)", targets.Revision())
	}
	if got := log.outcomes(); got["accepted"] != 1 || got["rejected"] != 1 {
		t.Fatalf("audit outcomes = %v", got)
	}
}

func TestPublishReplaysStuckPublishingAfterCrashWindow(t *testing.T) {
	_, targets, planner, publisher, log := newHarness(t, 0)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70})

	published, err := publisher.Publish(plan.ID, "alice")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Simulate a process that died after the atomic commit but before
	// finalizing: plan record is back in "publishing" while targets carry the
	// committed revision.
	if _, err := planner.Transition(plan.ID, []PlanStatus{PlanPublished}, func(pl *Plan) {
		pl.Status = PlanPublishing
	}); err != nil {
		t.Fatalf("force publishing: %v", err)
	}
	revisionBefore := targets.Revision()

	retry, err := publisher.Publish(plan.ID, "alice")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if retry.Status != PlanPublished {
		t.Fatalf("replay status = %s", retry.Status)
	}
	if targets.Revision() != revisionBefore {
		t.Fatalf("revision changed during replay: %d -> %d", revisionBefore, targets.Revision())
	}
	if retry.CommittedRevision != published.CommittedRevision {
		t.Fatalf("replay revision = %d, want %d", retry.CommittedRevision, published.CommittedRevision)
	}
	if got := log.outcomes(); got["accepted"] != 1 {
		t.Fatalf("accepted entries = %v, want 1", got)
	}
}

func TestPublishUnknownPlan(t *testing.T) {
	_, _, _, publisher, _ := newHarness(t, 0)
	_, err := publisher.Publish("nope", "alice")
	var publishErr *PublishError
	if !errors.As(err, &publishErr) || publishErr.Code != PublishNotFound {
		t.Fatalf("err = %v, want not_found", err)
	}
}
