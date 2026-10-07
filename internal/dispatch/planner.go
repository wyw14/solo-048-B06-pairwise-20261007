package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/station"
)

// ErrPlanNotFound is returned when a plan ID is unknown.
var ErrPlanNotFound = errors.New("plan not found")

// PlanStateError indicates that a transition was attempted against a plan
// whose status does not permit it. The embedded plan reflects the real current
// state so callers can surface it instead of blindly retrying.
type PlanStateError struct {
	Plan   Plan
	Status PlanStatus
}

func (e *PlanStateError) Error() string {
	return fmt.Sprintf("plan %s is %s", e.Plan.ID, e.Status)
}

func (e *PlanStateError) Is(target error) bool {
	_, ok := target.(*PlanStateError)
	return ok
}

type Planner struct {
	targets *station.TargetService
	mu      sync.RWMutex
	plans   map[string]Plan
}

func NewPlanner(targets *station.TargetService) *Planner {
	return &Planner{targets: targets, plans: make(map[string]Plan)}
}

func (p *Planner) Begin(ctx context.Context, requested map[string]float64, delay time.Duration) (Plan, error) {
	if len(requested) == 0 {
		return Plan{}, fmt.Errorf("at least one station target is required")
	}
	plan := Plan{
		ID: uuid.NewString(), SourceRevision: p.targets.Revision(), Targets: make(map[string]float64, len(requested)),
		Status: PlanCalculating, CreatedAt: time.Now().UTC(),
	}
	for stationID, celsius := range requested {
		plan.Targets[stationID] = celsius
	}
	p.save(plan)
	select {
	case <-ctx.Done():
		plan.Status = PlanFailed
		plan.Message = ctx.Err().Error()
		p.save(plan)
		return plan, ctx.Err()
	case <-time.After(delay):
	}
	return plan, nil
}

func (p *Planner) save(plan Plan) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.plans[plan.ID] = plan.Clone()
}

// Transition atomically changes a plan only while its status is one of from.
// change mutates the plan in place; the result is stored only if the status
// guard passes. It returns the current plan without storing it when the guard
// fails, wrapped in *PlanStateError.
func (p *Planner) Transition(planID string, from []PlanStatus, change func(*Plan)) (Plan, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, ok := p.plans[planID]
	if !ok {
		return Plan{}, fmt.Errorf("%w: %s", ErrPlanNotFound, planID)
	}
	permitted := false
	for _, status := range from {
		if plan.Status == status {
			permitted = true
			break
		}
	}
	if !permitted {
		return plan.Clone(), &PlanStateError{Plan: plan.Clone(), Status: plan.Status}
	}
	updated := plan.Clone()
	change(&updated)
	p.plans[planID] = updated.Clone()
	return updated.Clone(), nil
}

func (p *Planner) Plan(planID string) (Plan, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	plan, ok := p.plans[planID]
	return plan.Clone(), ok
}

func (p *Planner) Plans() []Plan {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]Plan, 0, len(p.plans))
	for _, plan := range p.plans {
		result = append(result, plan.Clone())
	}
	return result
}
