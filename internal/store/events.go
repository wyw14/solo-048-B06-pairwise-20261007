package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Event struct {
	ID       string         `json:"id"`
	Kind     string         `json:"kind"`
	EntityID string         `json:"entity_id"`
	Message  string         `json:"message"`
	At       time.Time      `json:"at"`
	Fields   map[string]any `json:"fields,omitempty"`
}

type EventLog struct {
	path string
	mu   sync.Mutex
}

func NewEventLog(path string) *EventLog {
	return &EventLog{path: path}
}

var ErrEventExists = errors.New("event already recorded")

func (l *EventLog) Append(event Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	if event.ID != "" {
		exists, err := l.containsLocked(event.ID)
		if err != nil {
			return err
		}
		if exists {
			return ErrEventExists
		}
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return fmt.Errorf("create event directory: %w", err)
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open event log: %w", err)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(event); err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return file.Sync()
}

// Contains reports whether an event with the given ID is already in the log.
func (l *EventLog) Contains(id string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.containsLocked(id)
}

func (l *EventLog) containsLocked(id string) (bool, error) {
	file, err := os.Open(l.path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("open event log: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return false, fmt.Errorf("decode event: %w", err)
		}
		if event.ID == id {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("scan event log: %w", err)
	}
	return false, nil
}

func (l *EventLog) List(limit int) ([]Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	file, err := os.Open(l.path)
	if errors.Is(err, os.ErrNotExist) {
		return []Event{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open event log: %w", err)
	}
	defer file.Close()
	items := make([]Event, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("decode event: %w", err)
		}
		items = append(items, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan event log: %w", err)
	}
	if limit > 0 && len(items) > limit {
		items = items[len(items)-limit:]
	}
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
	return items, nil
}

func (l *EventLog) Count() (int, error) {
	items, err := l.List(0)
	if err != nil {
		return 0, err
	}
	return len(items), nil
}
