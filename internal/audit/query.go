package audit

import (
	"fmt"
	"time"

	"heatpulse/internal/store"
)

type eventLister interface {
	List(limit int) ([]store.Event, error)
}

type Query struct {
	log eventLister
}

func NewQuery(log eventLister) *Query {
	return &Query{log: log}
}

// Entry returns the audit entry with the given ID, if recorded.
func (q *Query) Entry(id string) (Entry, bool, error) {
	events, err := q.log.List(0)
	if err != nil {
		return Entry{}, false, err
	}
	for _, event := range events {
		if event.ID != id {
			continue
		}
		return decodeEntry(event), true, nil
	}
	return Entry{}, false, nil
}

func decodeEntry(event store.Event) Entry {
	entry := Entry{
		ID: event.ID, Message: event.Message, At: event.At,
		EntityID: event.EntityID, Details: map[string]any{},
	}
	if len(event.Kind) >= len("audit.") && event.Kind[:len("audit.")] == "audit." {
		entry.Action = event.Kind[len("audit."):]
	}
	if actor, ok := event.Fields["actor"].(string); ok {
		entry.Actor = actor
	}
	if outcome, ok := event.Fields["outcome"].(string); ok {
		entry.Outcome = Outcome(outcome)
	}
	if details, ok := event.Fields["details"].(map[string]any); ok {
		entry.Details = details
	}
	return entry
}

func (q *Query) Recent(filter Filter) ([]Entry, error) {
	events, err := q.log.List(0)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(events))
	for _, event := range events {
		if len(event.Kind) < len("audit.") || event.Kind[:len("audit.")] != "audit." {
			continue
		}
		entry := decodeEntry(event)
		if filter.Match(entry) {
			entries = append(entries, entry)
		}
		if filter.Limit > 0 && len(entries) >= filter.Limit {
			break
		}
	}
	return entries, nil
}

func (q *Query) Summary(since time.Time) (map[Outcome]int, error) {
	entries, err := q.Recent(Filter{})
	if err != nil {
		return nil, fmt.Errorf("query audit summary: %w", err)
	}
	counts := make(map[Outcome]int)
	for _, entry := range entries {
		if entry.At.Before(since) {
			continue
		}
		counts[entry.Outcome]++
	}
	return counts, nil
}
