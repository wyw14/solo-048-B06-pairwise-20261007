package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"heatpulse/internal/dispatch"
)

func (a *App) listPlans(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.Planner.Plans())
}

func (a *App) getPlan(w http.ResponseWriter, r *http.Request) {
	plan, ok := a.Planner.Plan(chi.URLParam(r, "planID"))
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("plan not found"))
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (a *App) createPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Targets     map[string]float64 `json:"targets"`
		DelayMillis int                `json:"delay_millis"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.Planner.Begin(r.Context(), input.Targets, time.Duration(input.DelayMillis)*time.Millisecond)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, plan)
}

// publishResponse carries the plan even on failure so callers can inspect what
// actually happened (committed revision, applied targets, message) and decide
// whether to recalculate or safely retry.
type publishResponse struct {
	Code string        `json:"code,omitempty"`
	Plan dispatch.Plan `json:"plan"`
}

func (a *App) publishPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.Publisher.Publish(chi.URLParam(r, "planID"), input.Actor)
	if err == nil {
		writeJSON(w, http.StatusOK, plan)
		return
	}
	var publishErr *dispatch.PublishError
	if !errors.As(err, &publishErr) {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusInternalServerError
	switch publishErr.Code {
	case dispatch.PublishNotFound:
		status = http.StatusNotFound
	case dispatch.PublishConflict:
		status = http.StatusConflict
	case dispatch.PublishRejected:
		// 422: targets were valid input but the stations refused the publish;
		// nothing was applied and a corrected retry is safe.
		status = http.StatusUnprocessableEntity
	case dispatch.PublishAuditUnavailable:
		// Targets and plan state are settled; only the audit trail is missing.
		status = http.StatusFailedDependency
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(responseEnvelope{Data: publishResponse{Code: string(publishErr.Code), Plan: publishErr.Plan}, Error: err.Error()})
}

func (a *App) applyHold(w http.ResponseWriter, r *http.Request) {
	var input struct {
		StationIDs []string `json:"station_ids"`
		Actor      string   `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	revision, err := a.Targets.ManualHold(input.StationIDs, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": revision, "stations": input.StationIDs})
}

func (a *App) releaseHold(w http.ResponseWriter, r *http.Request) {
	revision, err := a.Targets.ReleaseHold(chi.URLParam(r, "stationID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": revision})
}

func (a *App) bufferCommand(w http.ResponseWriter, r *http.Request) {
	var input struct {
		StationID string  `json:"station_id"`
		Epoch     uint64  `json:"epoch"`
		Sequence  uint64  `json:"sequence"`
		Celsius   float64 `json:"celsius"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	command, err := a.DispatchBuffer.Enqueue(input.StationID, input.Epoch, input.Sequence, input.Celsius)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, command)
}
