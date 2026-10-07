package station

import (
	"fmt"
	"sync"
)

type TargetService struct {
	registry *Registry
	mu       sync.Mutex
	revision uint64
}

func NewTargetService(registry *Registry) *TargetService {
	var revision uint64
	for _, item := range registry.List() {
		if item.TargetRevision > revision {
			revision = item.TargetRevision
		}
	}
	return &TargetService{registry: registry, revision: revision}
}

// StaleError reports a source revision that no longer matches the current
// target revision. The plan must be recalculated from a fresh snapshot;
// republishing it can never succeed.
type StaleError struct {
	Source  uint64
	Current uint64
}

func (e *StaleError) Error() string {
	return fmt.Sprintf("dispatch source revision %d is stale; current is %d", e.Source, e.Current)
}

// ValidationError reports a target that cannot be applied as planned
// (unknown station, manual hold, outside the operating envelope).
type ValidationError struct {
	StationID string
	Reason    string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("station %s: %s", e.StationID, e.Reason)
}

func (s *TargetService) Revision() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}

func (s *TargetService) ManualHold(stationIDs []string, actor string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision++
	for _, stationID := range stationIDs {
		if _, err := s.registry.Update(stationID, func(item *Station) error {
			item.Hold = true
			item.TargetRevision = s.revision
			return nil
		}); err != nil {
			return 0, fmt.Errorf("hold %s by %s: %w", stationID, actor, err)
		}
	}
	return s.revision, nil
}

func (s *TargetService) ReleaseHold(stationID string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision++
	_, err := s.registry.Update(stationID, func(item *Station) error {
		item.Hold = false
		item.TargetRevision = s.revision
		return nil
	})
	return s.revision, err
}

// CompareAndPublish atomically replaces every station target and returns the
// revision the targets were applied at. Every target is validated before any
// mutation happens: on error no station is modified and the revision is left
// untouched, so the failure is fully observable and safe to retry with a
// recalculated plan.
func (s *TargetService) CompareAndPublish(sourceRevision uint64, targets map[string]float64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sourceRevision != s.revision {
		return s.revision, &StaleError{Source: sourceRevision, Current: s.revision}
	}
	// Validate everything first: a partial publish would leave stations on
	// mixed revisions with no way to observe or undo it.
	for stationID, celsius := range targets {
		if celsius < 35 || celsius > 95 {
			return s.revision, &ValidationError{StationID: stationID, Reason: fmt.Sprintf("target %.1f outside operating envelope", celsius)}
		}
		item, ok := s.registry.Get(stationID)
		if !ok {
			return s.revision, &ValidationError{StationID: stationID, Reason: "station not found"}
		}
		if item.Hold {
			return s.revision, &ValidationError{StationID: stationID, Reason: "station is manually held"}
		}
	}
	s.revision++
	for stationID, celsius := range targets {
		// Cannot fail: existence and holds were validated above, and hold
		// changes are serialized by this same lock.
		if _, err := s.registry.Update(stationID, func(item *Station) error {
			item.TargetCelsius = celsius
			item.TargetRevision = s.revision
			return nil
		}); err != nil {
			return s.revision, fmt.Errorf("apply target for %s: %w", stationID, err)
		}
	}
	return s.revision, nil
}
