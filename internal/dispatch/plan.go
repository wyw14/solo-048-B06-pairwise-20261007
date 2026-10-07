package dispatch

import "time"

type PlanStatus string

const (
	PlanCalculating   PlanStatus = "calculating"
	PlanPublishing    PlanStatus = "publishing"
	PlanPublished     PlanStatus = "published"
	PlanPublishFailed PlanStatus = "publish_failed"
	PlanStale         PlanStatus = "stale"
	PlanFailed        PlanStatus = "failed"
)

type Plan struct {
	ID             string             `json:"id"`
	SourceRevision uint64             `json:"source_revision"`
	Targets        map[string]float64 `json:"targets"`
	Status         PlanStatus         `json:"status"`
	CreatedAt      time.Time          `json:"created_at"`
	PublishedAt    time.Time          `json:"published_at,omitempty"`
	Message        string             `json:"message,omitempty"`
	// CommittedRevision is the target revision produced by this plan's publish;
	// zero until targets have actually been committed.
	CommittedRevision uint64 `json:"committed_revision,omitempty"`
	// AppliedTargets holds exactly the values committed to stations, so a
	// caller can inspect the real partial result even if the final status
	// update or audit write is still outstanding.
	AppliedTargets map[string]float64 `json:"applied_targets,omitempty"`
}

func (p Plan) Clone() Plan {
	copy := p
	copy.Targets = make(map[string]float64, len(p.Targets))
	for stationID, value := range p.Targets {
		copy.Targets[stationID] = value
	}
	if p.AppliedTargets != nil {
		copy.AppliedTargets = make(map[string]float64, len(p.AppliedTargets))
		for stationID, value := range p.AppliedTargets {
			copy.AppliedTargets[stationID] = value
		}
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
