package station

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrStaleRevision indicates that a publish request was computed against an
// older target revision and must be recalculated. Nothing is applied.
var ErrStaleRevision = errors.New("dispatch source revision is stale")

// PublishResult reports what a successful CompareAndPublish actually applied.
type PublishResult struct {
	Revision uint64
	Applied  map[string]float64
}

type TargetService struct {
	registry  *Registry
	mu        sync.Mutex
	revision  uint64
	committed map[string]PublishResult
}

func NewTargetService(registry *Registry) *TargetService {
	var revision uint64
	for _, item := range registry.List() {
		if item.TargetRevision > revision {
			revision = item.TargetRevision
		}
	}
	return &TargetService{registry: registry, revision: revision, committed: make(map[string]PublishResult)}
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

// CompareAndPublish applies targets only when sourceRevision matches the
// current revision. key (the plan ID) makes the commit idempotent: a retry
// that re-enters after a crash between the commit and the caller's status
// update returns the previously committed result instead of applying twice.
// All targets are validated (existence, manual hold, temperature envelope)
// before any station changes, so a validation failure applies nothing and
// leaves the revision untouched. On success it is atomic: every target is
// applied under one revision, which is returned together with the values
// actually written.
func (s *TargetService) CompareAndPublish(key string, sourceRevision uint64, targets map[string]float64) (PublishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.committed[key]; ok {
		return PublishResult{Revision: prior.Revision, Applied: cloneApplied(prior.Applied)}, nil
	}
	if sourceRevision != s.revision {
		return PublishResult{}, fmt.Errorf("%w: source %d, current %d", ErrStaleRevision, sourceRevision, s.revision)
	}
	stationIDs := make([]string, 0, len(targets))
	for stationID := range targets {
		stationIDs = append(stationIDs, stationID)
	}
	sort.Strings(stationIDs)

	for _, stationID := range stationIDs {
		celsius := targets[stationID]
		item, ok := s.registry.Get(stationID)
		if !ok {
			return PublishResult{}, fmt.Errorf("station %s not found", stationID)
		}
		if item.Hold {
			return PublishResult{}, fmt.Errorf("station %s is manually held", stationID)
		}
		if celsius < 35 || celsius > 95 {
			return PublishResult{}, fmt.Errorf("target %.1f for %s outside operating envelope", celsius, stationID)
		}
	}

	nextRevision := s.revision + 1
	applied := make(map[string]float64, len(stationIDs))
	at := time.Now().UTC()
	err := s.registry.UpdateMany(stationIDs, func(item *Station) error {
		if item.Hold {
			return fmt.Errorf("station %s is manually held", item.ID)
		}
		item.TargetCelsius = targets[item.ID]
		item.TargetRevision = nextRevision
		item.UpdatedAt = at
		return nil
	})
	if err != nil {
		return PublishResult{}, err
	}
	for _, stationID := range stationIDs {
		applied[stationID] = targets[stationID]
	}
	s.revision = nextRevision
	result := PublishResult{Revision: nextRevision, Applied: cloneApplied(applied)}
	s.committed[key] = result
	return result, nil
}

func cloneApplied(applied map[string]float64) map[string]float64 {
	copy := make(map[string]float64, len(applied))
	for stationID, celsius := range applied {
		copy[stationID] = celsius
	}
	return copy
}
