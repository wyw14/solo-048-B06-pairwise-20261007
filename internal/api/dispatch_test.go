package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"heatpulse/internal/station"
)

func setupApp(t *testing.T) (*App, http.Handler) {
	t.Helper()
	app, err := NewApp(t.TempDir())
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	return app, app.Handler()
}

func doJSON(t *testing.T, handler http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var envelope struct {
		Data  map[string]any `json:"data"`
		Error string         `json:"error"`
	}
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("decode %s body %q: %v", path, rec.Body.String(), err)
		}
	}
	return rec.Code, map[string]any{"data": envelope.Data, "error": envelope.Error}
}

func createPlan(t *testing.T, handler http.Handler, targets map[string]float64) string {
	t.Helper()
	status, body := doJSON(t, handler, http.MethodPost, "/api/v1/dispatch/plans", map[string]any{"targets": targets})
	if status != http.StatusAccepted {
		t.Fatalf("create plan status = %d body = %v", status, body)
	}
	return body["data"].(map[string]any)["id"].(string)
}

func TestPublishPlanHTTPSuccessAndIdempotentRetry(t *testing.T) {
	_, handler := setupApp(t)
	planID := createPlan(t, handler, map[string]float64{"xincheng": 70})

	status, body := doJSON(t, handler, http.MethodPost, "/api/v1/dispatch/plans/"+planID+"/publish", map[string]any{"actor": "alice"})
	if status != http.StatusOK {
		t.Fatalf("publish status = %d body = %v", status, body)
	}
	data := body["data"].(map[string]any)
	if data["status"] != "published" {
		t.Fatalf("status = %v", data["status"])
	}

	// Immediate retry: still 200, no conflict, even though targets are live.
	status, _ = doJSON(t, handler, http.MethodPost, "/api/v1/dispatch/plans/"+planID+"/publish", map[string]any{"actor": "alice"})
	if status != http.StatusOK {
		t.Fatalf("idempotent republish status = %d, want 200", status)
	}
}

func TestPublishPlanHTTPStaleReturnsConflictWithPlan(t *testing.T) {
	app, handler := setupApp(t)
	planID := createPlan(t, handler, map[string]float64{"binhe": 70})

	// Move the revision out from under the calculated plan.
	if _, err := app.Targets.ManualHold([]string{"nanhu"}, "bob"); err != nil {
		t.Fatalf("hold: %v", err)
	}
	status, body := doJSON(t, handler, http.MethodPost, "/api/v1/dispatch/plans/"+planID+"/publish", map[string]any{"actor": "alice"})
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body = %v", status, body)
	}
	data := body["data"].(map[string]any)
	if data["code"] != "conflict" {
		t.Fatalf("code = %v, want conflict", data["code"])
	}
	plan := data["plan"].(map[string]any)
	if plan["status"] != "stale" {
		t.Fatalf("plan status = %v, want stale", plan["status"])
	}
	if applied := plan["applied_targets"]; applied != nil {
		t.Fatalf("applied_targets = %v, want absent on stale", applied)
	}

	// GET surfaces the same authoritative state for the caller.
	status, body = doJSON(t, handler, http.MethodGet, "/api/v1/dispatch/plans/"+planID, nil)
	if status != http.StatusOK {
		t.Fatalf("get plan status = %d", status)
	}
	if body["data"].(map[string]any)["status"] != "stale" {
		t.Fatalf("stored plan not stale: %v", body["data"])
	}
}

func TestPublishPlanHTTPHeldStationReturnsUnprocessable(t *testing.T) {
	app, handler := setupApp(t)
	planID := createPlan(t, handler, map[string]float64{"nanhu": 70, "xijiao": 71})

	// Hold a planned station without moving the target revision, so the
	// revision guard passes but the atomic commit refuses and applies nothing.
	if _, err := app.Stations.Update("nanhu", func(item *station.Station) error {
		item.Hold = true
		return nil
	}); err != nil {
		t.Fatalf("hold: %v", err)
	}
	status, body := doJSON(t, handler, http.MethodPost, "/api/v1/dispatch/plans/"+planID+"/publish", map[string]any{"actor": "alice"})
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d want 422 body = %v", status, body)
	}
	data := body["data"].(map[string]any)
	if data["code"] != "rejected" {
		t.Fatalf("code = %v, want rejected", data["code"])
	}
	plan := data["plan"].(map[string]any)
	if plan["status"] != "publish_failed" {
		t.Fatalf("plan status = %v, want publish_failed", plan["status"])
	}
	if plan["applied_targets"] != nil {
		t.Fatalf("applied_targets = %v, want absent on rejection", plan["applied_targets"])
	}

	// Clear the hold and safely retry the same plan.
	if _, err := app.Stations.Update("nanhu", func(item *station.Station) error {
		item.Hold = false
		return nil
	}); err != nil {
		t.Fatalf("release: %v", err)
	}
	status, _ = doJSON(t, handler, http.MethodPost, "/api/v1/dispatch/plans/"+planID+"/publish", map[string]any{"actor": "alice"})
	if status != http.StatusOK {
		t.Fatalf("retry status = %d, want 200", status)
	}
}

func TestPublishPlanHTTPUnknown(t *testing.T) {
	_, handler := setupApp(t)
	status, body := doJSON(t, handler, http.MethodPost, "/api/v1/dispatch/plans/ghost/publish", map[string]any{"actor": "alice"})
	if status != http.StatusNotFound {
		t.Fatalf("status = %d want 404 body = %v", status, body)
	}
}
