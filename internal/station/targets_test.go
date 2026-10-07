package station

import (
	"errors"
	"testing"
)

func newTestTargets(stations ...Station) (*Registry, *TargetService) {
	registry := NewRegistry(stations)
	return registry, NewTargetService(registry)
}

func TestCompareAndPublishAppliesAllTargetsAtomically(t *testing.T) {
	registry, targets := newTestTargets(
		Station{ID: "alpha", TargetCelsius: 60, TargetRevision: 1},
		Station{ID: "bravo", TargetCelsius: 62, TargetRevision: 1},
	)

	revision, err := targets.CompareAndPublish(1, map[string]float64{"alpha": 70, "bravo": 72})
	if err != nil {
		t.Fatalf("CompareAndPublish: %v", err)
	}
	if revision != 2 {
		t.Fatalf("applied revision = %d, want 2", revision)
	}
	for id, want := range map[string]float64{"alpha": 70, "bravo": 72} {
		item, _ := registry.Get(id)
		if item.TargetCelsius != want || item.TargetRevision != 2 {
			t.Errorf("%s: target %.1f rev %d, want %.1f rev 2", id, item.TargetCelsius, item.TargetRevision, want)
		}
	}
}

func TestCompareAndPublishValidationFailureLeavesEverythingUntouched(t *testing.T) {
	registry, targets := newTestTargets(
		Station{ID: "alpha", TargetCelsius: 60, TargetRevision: 1},
		Station{ID: "bravo", TargetCelsius: 62, TargetRevision: 1},
	)

	// One target outside the envelope: nothing may be applied, not even the
	// valid target, and the revision must not move.
	_, err := targets.CompareAndPublish(1, map[string]float64{"alpha": 70, "bravo": 120})
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("error = %v, want ValidationError", err)
	}
	if targets.Revision() != 1 {
		t.Fatalf("revision = %d, want 1 (unchanged)", targets.Revision())
	}
	for id, want := range map[string]float64{"alpha": 60, "bravo": 62} {
		item, _ := registry.Get(id)
		if item.TargetCelsius != want || item.TargetRevision != 1 {
			t.Errorf("%s: target %.1f rev %d, want %.1f rev 1 (untouched)", id, item.TargetCelsius, item.TargetRevision, want)
		}
	}
}

func TestCompareAndPublishHeldStationLeavesEverythingUntouched(t *testing.T) {
	registry, targets := newTestTargets(
		Station{ID: "alpha", TargetCelsius: 60, TargetRevision: 1},
		Station{ID: "bravo", TargetCelsius: 62, TargetRevision: 1},
	)
	if _, err := targets.ManualHold([]string{"bravo"}, "test"); err != nil {
		t.Fatalf("ManualHold: %v", err)
	}

	_, err := targets.CompareAndPublish(2, map[string]float64{"alpha": 70, "bravo": 72})
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("error = %v, want ValidationError", err)
	}
	if targets.Revision() != 2 {
		t.Fatalf("revision = %d, want 2 (unchanged)", targets.Revision())
	}
	item, _ := registry.Get("alpha")
	if item.TargetCelsius != 60 {
		t.Errorf("alpha target = %.1f, want 60 (untouched)", item.TargetCelsius)
	}
}

func TestCompareAndPublishStaleRevision(t *testing.T) {
	registry, targets := newTestTargets(Station{ID: "alpha", TargetCelsius: 60, TargetRevision: 3})

	_, err := targets.CompareAndPublish(2, map[string]float64{"alpha": 70})
	var stale *StaleError
	if !errors.As(err, &stale) {
		t.Fatalf("error = %v, want StaleError", err)
	}
	if stale.Source != 2 || stale.Current != 3 {
		t.Errorf("StaleError = %+v, want source 2 current 3", stale)
	}
	if targets.Revision() != 3 {
		t.Fatalf("revision = %d, want 3 (unchanged)", targets.Revision())
	}
	item, _ := registry.Get("alpha")
	if item.TargetCelsius != 60 {
		t.Errorf("alpha target = %.1f, want 60 (untouched)", item.TargetCelsius)
	}
}
