package dispatch

import "time"

type PlanStatus string

const (
	PlanCalculating PlanStatus = "calculating"
	PlanPublished   PlanStatus = "published"
	PlanStale       PlanStatus = "stale"
	PlanFailed      PlanStatus = "failed"
)

type Plan struct {
	ID             string             `json:"id"`
	SourceRevision uint64             `json:"source_revision"`
	Targets        map[string]float64 `json:"targets"`
	Status         PlanStatus         `json:"status"`
	CreatedAt      time.Time          `json:"created_at"`
	PublishedAt    time.Time          `json:"published_at,omitempty"`
	// AppliedRevision is the target revision the plan landed at; zero until
	// the targets were actually committed.
	AppliedRevision uint64 `json:"applied_revision,omitempty"`
	// AuditedAt marks when the terminal audit record was written. A terminal
	// plan (published/stale/failed) with a zero AuditedAt has an audit gap,
	// described by AuditError, which a Publish retry backfills.
	AuditedAt  time.Time `json:"audited_at,omitempty"`
	Message    string    `json:"message,omitempty"`
	AuditError string    `json:"audit_error,omitempty"`
}

func (p Plan) Clone() Plan {
	copy := p
	copy.Targets = make(map[string]float64, len(p.Targets))
	for stationID, value := range p.Targets {
		copy.Targets[stationID] = value
	}
	return copy
}

type BufferedCommand struct {
	StationID string    `json:"station_id"`
	CommandID string    `json:"command_id"`
	Epoch     uint64    `json:"epoch"`
	Sequence  uint64    `json:"sequence"`
	Celsius   float64   `json:"celsius"`
	QueuedAt  time.Time `json:"queued_at"`
}
