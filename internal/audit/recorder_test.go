package audit

import (
	"errors"
	"testing"

	"heatpulse/internal/store"
)

type memStore struct {
	events []store.Event
	failOn map[string]bool
}

func (m *memStore) Append(event store.Event) error {
	for _, existing := range m.events {
		if existing.ID == event.ID {
			return store.ErrEventExists
		}
	}
	if m.failOn[event.ID] {
		return errors.New("disk full")
	}
	// Mirror JSON persistence: defined string types come back as plain strings.
	stored := event
	if fields := event.Fields; fields != nil {
		stored.Fields = map[string]any{}
		for key, value := range fields {
			if outcome, ok := value.(Outcome); ok {
				value = string(outcome)
			}
			stored.Fields[key] = value
		}
	}
	m.events = append(m.events, stored)
	return nil
}

func (m *memStore) Contains(id string) (bool, error) {
	for _, event := range m.events {
		if event.ID == id {
			return true, nil
		}
	}
	return false, nil
}

func (m *memStore) List(limit int) ([]store.Event, error) {
	events := make([]store.Event, len(m.events))
	copy(events, m.events)
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	return events, nil
}

func TestRecordOnceIdempotent(t *testing.T) {
	log := &memStore{}
	recorder := NewRecorder(log)
	entry := Entry{Action: "dispatch.publish", EntityID: "plan-1", Outcome: OutcomeAccepted}

	first, recorded, err := recorder.RecordOnce("fixed-id", entry)
	if err != nil || !recorded {
		t.Fatalf("first record = (%v, %v, %v)", first, recorded, err)
	}
	_, recorded, err = recorder.RecordOnce("fixed-id", entry)
	if err != nil || recorded {
		t.Fatalf("duplicate record = (recorded %v, err %v), want (false, nil)", recorded, err)
	}
	if len(log.events) != 1 {
		t.Fatalf("events = %d, want 1", len(log.events))
	}

	query := NewQuery(log)
	got, ok, err := query.Entry("fixed-id")
	if err != nil || !ok {
		t.Fatalf("query entry = (%v, %v, %v)", got, ok, err)
	}
	if got.Outcome != OutcomeAccepted || got.EntityID != "plan-1" {
		t.Fatalf("decoded entry = %+v", got)
	}
}

func TestRecordOncePropagatesStorageError(t *testing.T) {
	log := &memStore{failOn: map[string]bool{"fixed-id": true}}
	recorder := NewRecorder(log)
	_, _, err := recorder.RecordOnce("fixed-id", Entry{Action: "x", EntityID: "y"})
	if err == nil {
		t.Fatal("expected storage error")
	}
	exists, err := recorder.Exists("fixed-id")
	if err != nil || exists {
		t.Fatalf("Exists = (%v, %v), want (false, nil)", exists, err)
	}
}
