package station

import "testing"

func newTestService() (*Registry, *TargetService) {
	registry := NewRegistry([]Station{
		{ID: "s1", TargetCelsius: 60, TargetRevision: 1},
		{ID: "s2", TargetCelsius: 62, TargetRevision: 1},
	})
	return registry, NewTargetService(registry)
}

func TestCompareAndPublishAppliesAllUnderOneRevision(t *testing.T) {
	registry, service := newTestService()

	result, err := service.CompareAndPublish("plan-1", 1, map[string]float64{"s1": 70, "s2": 71})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if result.Revision != 2 {
		t.Fatalf("revision = %d, want 2", result.Revision)
	}
	if result.Applied["s1"] != 70 || result.Applied["s2"] != 71 {
		t.Fatalf("applied = %v", result.Applied)
	}
	for _, id := range []string{"s1", "s2"} {
		item, _ := registry.Get(id)
		if item.TargetRevision != 2 {
			t.Fatalf("%s revision = %d, want 2", id, item.TargetRevision)
		}
	}
}

func TestCompareAndPublishValidationFailureAppliesNothing(t *testing.T) {
	registry, service := newTestService()

	// s2 is outside the envelope: s1 must not be touched either.
	_, err := service.CompareAndPublish("plan-bad", 1, map[string]float64{"s1": 70, "s2": 200})
	if err == nil {
		t.Fatal("expected envelope error")
	}
	if service.Revision() != 1 {
		t.Fatalf("revision = %d, want unchanged 1", service.Revision())
	}
	item, _ := registry.Get("s1")
	if item.TargetCelsius != 60 || item.TargetRevision != 1 {
		t.Fatalf("s1 mutated on failed publish: %+v", item)
	}
	item, _ = registry.Get("s2")
	if item.TargetCelsius != 62 || item.TargetRevision != 1 {
		t.Fatalf("s2 mutated on failed publish: %+v", item)
	}
}

func TestCompareAndPublishStaleRevisionAppliesNothing(t *testing.T) {
	registry, service := newTestService()

	if _, err := service.CompareAndPublish("plan-1", 1, map[string]float64{"s1": 70}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	_, err := service.CompareAndPublish("plan-2", 1, map[string]float64{"s2": 71})
	if err == nil {
		t.Fatal("expected stale revision error")
	}
	item, _ := registry.Get("s2")
	if item.TargetCelsius != 62 {
		t.Fatalf("s2 mutated on stale publish: %+v", item)
	}
}

func TestCompareAndPublishIdempotentByKey(t *testing.T) {
	service := newTestService2(t)

	first, err := service.CompareAndPublish("plan-1", 1, map[string]float64{"s1": 70})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	// A retry that lost the race against the status update re-enters with the
	// original source revision: it must return the same result, not apply
	// again or report stale.
	second, err := service.CompareAndPublish("plan-1", 1, map[string]float64{"s1": 70})
	if err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if second.Revision != first.Revision {
		t.Fatalf("retry revision = %d, want %d", second.Revision, first.Revision)
	}
	if service.Revision() != 2 {
		t.Fatalf("revision = %d, want 2 (commit happened exactly once)", service.Revision())
	}
}

func TestCompareAndPublishHeldStationAppliesNothing(t *testing.T) {
	registry, service := newTestService()
	if _, err := service.ManualHold([]string{"s1"}, "op"); err != nil {
		t.Fatalf("hold: %v", err)
	}
	_, err := service.CompareAndPublish("plan-held", service.Revision(), map[string]float64{"s1": 70, "s2": 71})
	if err == nil {
		t.Fatal("expected hold error")
	}
	item, _ := registry.Get("s2")
	if item.TargetCelsius != 62 {
		t.Fatalf("s2 mutated when sibling was held: %+v", item)
	}
}

func newTestService2(t *testing.T) *TargetService {
	t.Helper()
	_, service := newTestService()
	return service
}
