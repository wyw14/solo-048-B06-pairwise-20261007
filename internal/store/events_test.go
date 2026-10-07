package store

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestEventLogAppendIdempotentByID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	log := NewEventLog(path)

	event := Event{ID: "evt-1", Kind: "audit.x", EntityID: "p1"}
	if err := log.Append(event); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := log.Append(event); !errors.Is(err, ErrEventExists) {
		t.Fatalf("duplicate append err = %v, want ErrEventExists", err)
	}
	contains, err := log.Contains("evt-1")
	if err != nil || !contains {
		t.Fatalf("Contains = (%v, %v), want (true, nil)", contains, err)
	}
	contains, err = log.Contains("missing")
	if err != nil || contains {
		t.Fatalf("Contains missing = (%v, %v), want (false, nil)", contains, err)
	}
	events, err := log.List(0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want exactly 1", len(events))
	}
}
