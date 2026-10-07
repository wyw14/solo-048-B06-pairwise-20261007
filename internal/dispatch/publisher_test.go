package dispatch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
)

func newTestRig(logPath string) (*station.Registry, *station.TargetService, *Planner) {
	registry := station.NewRegistry([]station.Station{
		{ID: "alpha", TargetCelsius: 60, TargetRevision: 1},
		{ID: "bravo", TargetCelsius: 62, TargetRevision: 1},
	})
	targets := station.NewTargetService(registry)
	return registry, targets, NewPlanner(targets)
}

func newPublisher(planner *Planner, targets *station.TargetService, logPath string) *Publisher {
	return NewPublisher(planner, targets, audit.NewRecorder(store.NewEventLog(logPath)))
}

func publishEntries(t *testing.T, logPath string, planID string) []audit.Entry {
	t.Helper()
	entries, err := audit.NewQuery(store.NewEventLog(logPath)).Recent(audit.Filter{EntityID: planID, Action: "dispatch.publish"})
	if err != nil {
		t.Fatalf("query audit: %v", err)
	}
	return entries
}

// failingLogPath returns an event-log path that always fails to append: its
// parent is a regular file, so creating the log directory is impossible.
func failingLogPath(t *testing.T) string {
	t.Helper()
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	return filepath.Join(blocker, "events.jsonl")
}

func TestPublishAppliesTargetsAndRecordsAudit(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "events.jsonl")
	registry, targets, planner := newTestRig(logPath)
	publisher := newPublisher(planner, targets, logPath)

	plan, err := planner.Begin(context.Background(), map[string]float64{"alpha": 70, "bravo": 72}, 0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	published, err := publisher.Publish(plan.ID, "operator")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if published.Status != PlanPublished {
		t.Fatalf("status = %s, want published", published.Status)
	}
	if published.AppliedRevision != 2 {
		t.Errorf("applied revision = %d, want 2", published.AppliedRevision)
	}
	if published.AuditedAt.IsZero() || published.AuditError != "" {
		t.Errorf("audit state = (%v, %q), want recorded without error", published.AuditedAt, published.AuditError)
	}
	item, _ := registry.Get("alpha")
	if item.TargetCelsius != 70 || item.TargetRevision != 2 {
		t.Errorf("alpha target = %.1f rev %d, want 70 rev 2", item.TargetCelsius, item.TargetRevision)
	}
	entries := publishEntries(t, logPath, plan.ID)
	if len(entries) != 1 || entries[0].Outcome != audit.OutcomeAccepted {
		t.Fatalf("audit entries = %+v, want one accepted", entries)
	}
}

func TestPublishStaleRevisionMarksPlanStaleAndStaysRetryable(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "events.jsonl")
	registry, targets, planner := newTestRig(logPath)
	publisher := newPublisher(planner, targets, logPath)

	plan, err := planner.Begin(context.Background(), map[string]float64{"alpha": 70}, 0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// Move the target revision after the plan was calculated.
	if _, err := targets.ManualHold([]string{"alpha"}, "operator"); err != nil {
		t.Fatalf("ManualHold: %v", err)
	}
	if _, err := targets.ReleaseHold("alpha"); err != nil {
		t.Fatalf("ReleaseHold: %v", err)
	}

	got, err := publisher.Publish(plan.ID, "operator")
	if err == nil {
		t.Fatal("Publish: expected stale error, got nil")
	}
	if got.Status != PlanStale {
		t.Fatalf("status = %s, want stale", got.Status)
	}
	item, _ := registry.Get("alpha")
	if item.TargetCelsius != 60 {
		t.Errorf("alpha target = %.1f, want 60 (untouched)", item.TargetCelsius)
	}

	// Retrying reports the stored terminal outcome without changing anything.
	again, err := publisher.Publish(plan.ID, "operator")
	if err == nil {
		t.Fatal("retry: expected conflict error, got nil")
	}
	if again.Status != PlanStale || again.Message != got.Message {
		t.Errorf("retry plan = (%s, %q), want stored stale outcome", again.Status, again.Message)
	}
	entries := publishEntries(t, logPath, plan.ID)
	if len(entries) != 1 || entries[0].Outcome != audit.OutcomeRejected {
		t.Fatalf("audit entries = %+v, want exactly one rejected", entries)
	}
}

func TestPublishInvalidTargetMarksPlanFailedNotStale(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "events.jsonl")
	_, targets, planner := newTestRig(logPath)
	publisher := newPublisher(planner, targets, logPath)

	plan, err := planner.Begin(context.Background(), map[string]float64{"alpha": 120}, 0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	got, err := publisher.Publish(plan.ID, "operator")
	if err == nil {
		t.Fatal("Publish: expected validation error, got nil")
	}
	if got.Status != PlanFailed {
		t.Fatalf("status = %s, want failed (a validation error is not staleness)", got.Status)
	}
	if targets.Revision() != 1 {
		t.Errorf("revision = %d, want 1 (unchanged)", targets.Revision())
	}
	entries := publishEntries(t, logPath, plan.ID)
	if len(entries) != 1 || entries[0].Outcome != audit.OutcomeRejected {
		t.Fatalf("audit entries = %+v, want one rejected", entries)
	}
}

func TestPublishAuditFailureKeepsTrueStateAndRetryBackfills(t *testing.T) {
	badLog := failingLogPath(t)
	registry, targets, planner := newTestRig(badLog)
	publisher := newPublisher(planner, targets, badLog)

	plan, err := planner.Begin(context.Background(), map[string]float64{"alpha": 70}, 0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// The audit log is down, but the state change itself succeeds: the
	// publish must not be reported as failed, and the gap must be visible
	// on the plan.
	published, err := publisher.Publish(plan.ID, "operator")
	if err != nil {
		t.Fatalf("Publish: audit failure must not fail the publish, got %v", err)
	}
	if published.Status != PlanPublished {
		t.Fatalf("status = %s, want published", published.Status)
	}
	if !published.AuditedAt.IsZero() || published.AuditError == "" {
		t.Errorf("audit state = (%v, %q), want pending record with error", published.AuditedAt, published.AuditError)
	}
	item, _ := registry.Get("alpha")
	if item.TargetCelsius != 70 {
		t.Errorf("alpha target = %.1f, want 70 (applied)", item.TargetCelsius)
	}

	// Retry once the log is healthy again: the caller gets the published
	// plan (no false conflict), the audit record is backfilled, and the
	// targets are not applied a second time.
	goodLog := filepath.Join(t.TempDir(), "events.jsonl")
	retry := newPublisher(planner, targets, goodLog)
	again, err := retry.Publish(plan.ID, "operator")
	if err != nil {
		t.Fatalf("retry Publish: %v", err)
	}
	if again.Status != PlanPublished {
		t.Fatalf("retry status = %s, want published", again.Status)
	}
	if again.AuditedAt.IsZero() || again.AuditError != "" {
		t.Errorf("retry audit state = (%v, %q), want backfilled without error", again.AuditedAt, again.AuditError)
	}
	if targets.Revision() != 2 {
		t.Errorf("revision = %d, want 2 (targets applied exactly once)", targets.Revision())
	}
	entries := publishEntries(t, goodLog, plan.ID)
	if len(entries) != 1 || entries[0].Outcome != audit.OutcomeAccepted {
		t.Fatalf("audit entries = %+v, want one accepted (backfilled)", entries)
	}
}

func TestPublishRetryIsIdempotent(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "events.jsonl")
	_, targets, planner := newTestRig(logPath)
	publisher := newPublisher(planner, targets, logPath)

	plan, err := planner.Begin(context.Background(), map[string]float64{"alpha": 70}, 0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	first, err := publisher.Publish(plan.ID, "operator")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	second, err := publisher.Publish(plan.ID, "operator")
	if err != nil {
		t.Fatalf("retry Publish: %v", err)
	}
	if second.Status != PlanPublished || second.AppliedRevision != first.AppliedRevision {
		t.Errorf("retry = (%s, rev %d), want (%s, rev %d)", second.Status, second.AppliedRevision, first.Status, first.AppliedRevision)
	}
	if targets.Revision() != 2 {
		t.Errorf("revision = %d, want 2 (targets applied exactly once)", targets.Revision())
	}
	entries := publishEntries(t, logPath, plan.ID)
	if len(entries) != 1 {
		t.Fatalf("audit entries = %d, want exactly 1", len(entries))
	}
}

func TestPublishUnknownPlan(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "events.jsonl")
	_, targets, planner := newTestRig(logPath)
	publisher := newPublisher(planner, targets, logPath)

	if _, err := publisher.Publish("missing", "operator"); !errors.Is(err, ErrPlanNotFound) {
		t.Fatalf("error = %v, want ErrPlanNotFound", err)
	}
}
