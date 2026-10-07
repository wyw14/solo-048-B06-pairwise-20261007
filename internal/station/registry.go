package station

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Registry struct {
	mu       sync.RWMutex
	stations map[string]Station
}

func NewRegistry(stations []Station) *Registry {
	r := &Registry{stations: make(map[string]Station, len(stations))}
	for _, item := range stations {
		if item.Exchangers == nil {
			item.Exchangers = make(map[string]ExchangerPhase)
		}
		item.UpdatedAt = time.Now().UTC()
		r.stations[item.ID] = item.Clone()
	}
	return r
}

func (r *Registry) Get(stationID string) (Station, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.stations[stationID]
	return item.Clone(), ok
}

func (r *Registry) List() []Station {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := make([]Station, 0, len(r.stations))
	for _, item := range r.stations {
		items = append(items, item.Clone())
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items
}

func (r *Registry) Update(stationID string, change func(*Station) error) (Station, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.updateLocked(stationID, change)
}

func (r *Registry) updateLocked(stationID string, change func(*Station) error) (Station, error) {
	item, ok := r.stations[stationID]
	if !ok {
		return Station{}, fmt.Errorf("station %s not found", stationID)
	}
	item = item.Clone()
	if err := change(&item); err != nil {
		return Station{}, err
	}
	item.UpdatedAt = time.Now().UTC()
	r.stations[stationID] = item.Clone()
	return item, nil
}

// UpdateMany applies change to every station under a single lock. Every change
// runs on a clone first; if any of them fails nothing is committed.
func (r *Registry) UpdateMany(stationIDs []string, change func(*Station) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	updated := make(map[string]Station, len(stationIDs))
	for _, stationID := range stationIDs {
		item, ok := r.stations[stationID]
		if !ok {
			return fmt.Errorf("station %s not found", stationID)
		}
		item = item.Clone()
		if err := change(&item); err != nil {
			return err
		}
		updated[stationID] = item
	}
	for _, item := range updated {
		item.UpdatedAt = time.Now().UTC()
		r.stations[item.ID] = item.Clone()
	}
	return nil
}

func (r *Registry) SetOnline(stationID string, online bool) (Station, error) {
	return r.Update(stationID, func(item *Station) error {
		item.Online = online
		return nil
	})
}

func (r *Registry) SetExchanger(stationID, exchangerID string, phase ExchangerPhase) (Station, error) {
	return r.Update(stationID, func(item *Station) error {
		if item.Exchangers == nil {
			item.Exchangers = make(map[string]ExchangerPhase)
		}
		item.Exchangers[exchangerID] = phase
		return nil
	})
}
