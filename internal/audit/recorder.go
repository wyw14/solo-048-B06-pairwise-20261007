package audit

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/store"
)

// eventStore is the subset of store.EventLog the recorder needs. It is an
// interface so callers can inject a failing store in tests.
type eventStore interface {
	Append(store.Event) error
	Contains(id string) (bool, error)
}

type Recorder struct {
	log eventStore
}

func NewRecorder(log eventStore) *Recorder {
	return &Recorder{log: log}
}

func (r *Recorder) Record(entry Entry) (Entry, error) {
	if entry.Action == "" || entry.EntityID == "" {
		return Entry{}, fmt.Errorf("audit action and entity are required")
	}
	if entry.ID == "" {
		entry.ID = uuid.NewString()
	}
	if entry.At.IsZero() {
		entry.At = time.Now().UTC()
	}
	event := store.Event{
		ID:       entry.ID,
		Kind:     "audit." + entry.Action,
		EntityID: entry.EntityID,
		Message:  entry.Message,
		At:       entry.At,
		Fields: map[string]any{
			"actor":   entry.Actor,
			"outcome": entry.Outcome,
			"details": entry.Details,
		},
	}
	if err := r.log.Append(event); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

// RecordOnce records an entry under a caller-changed deterministic ID. It is
// idempotent: a retry with the same ID after a crash or ambiguous write returns
// recorded=false instead of appending a duplicate.
func (r *Recorder) RecordOnce(id string, entry Entry) (Entry, bool, error) {
	if id == "" {
		return Entry{}, false, fmt.Errorf("audit entry id is required")
	}
	entry.ID = id
	recorded, err := r.Record(entry)
	if errors.Is(err, store.ErrEventExists) {
		return entry, false, nil
	}
	if err != nil {
		return Entry{}, false, err
	}
	return recorded, true, nil
}

// Exists reports whether an audit entry with the given ID was already recorded.
func (r *Recorder) Exists(id string) (bool, error) {
	return r.log.Contains(id)
}

func (r *Recorder) Accepted(action, entityID, actor, message string, details map[string]any) error {
	_, err := r.Record(Entry{Action: action, EntityID: entityID, Actor: actor, Outcome: OutcomeAccepted, Message: message, Details: details})
	return err
}

func (r *Recorder) Rejected(action, entityID, actor, message string, details map[string]any) error {
	_, err := r.Record(Entry{Action: action, EntityID: entityID, Actor: actor, Outcome: OutcomeRejected, Message: message, Details: details})
	return err
}
