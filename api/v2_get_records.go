package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/service"
)

// GET /api/v2/records/{id}
// GET /api/v2/records/{id}?version=N
// GetRecords retrieves the record at the latest or a specific version.
func (a *V2API) GetRecords(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := mux.Vars(r)["id"]

	idNumber, err := strconv.ParseInt(id, 10, 32)
	if err != nil || idNumber <= 0 {
		logError(writeError(w, "invalid id; id must be a positive number", http.StatusBadRequest))
		return
	}

	version := 0
	if vStr := r.URL.Query().Get("version"); vStr != "" {
		v, err := strconv.ParseInt(vStr, 10, 32)
		if err != nil || v <= 0 {
			logError(writeError(w, "invalid version; version must be a positive number", http.StatusBadRequest))
			return
		}
		version = int(v)
	}

	record, err := a.records.GetVersionedRecord(ctx, int(idNumber), version)
	if err != nil {
		if errors.Is(err, service.ErrRecordDoesNotExist) {
			logError(writeError(w, fmt.Sprintf("record of id %v does not exist", idNumber), http.StatusNotFound))
			return
		}
		if errors.Is(err, service.ErrRecordVersionDoesNotExist) {
			logError(writeError(w, fmt.Sprintf("record of id %v at version %v does not exist", idNumber, version), http.StatusNotFound))
			return
		}
		logError(err)
		logError(writeError(w, ErrInternal.Error(), http.StatusInternalServerError))
		return
	}

	logError(writeJSON(w, record, http.StatusOK))
}

// GET /api/v2/records/{id}/versions
// GetRecordVersions lists all versions of a record in ascending order.
func (a *V2API) GetRecordVersions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := mux.Vars(r)["id"]

	idNumber, err := strconv.ParseInt(id, 10, 32)
	if err != nil || idNumber <= 0 {
		logError(writeError(w, "invalid id; id must be a positive number", http.StatusBadRequest))
		return
	}

	versions, err := a.records.ListRecordVersions(ctx, int(idNumber))
	if err != nil {
		if errors.Is(err, service.ErrRecordDoesNotExist) {
			logError(writeError(w, fmt.Sprintf("record of id %v does not exist", idNumber), http.StatusNotFound))
			return
		}
		logError(err)
		logError(writeError(w, ErrInternal.Error(), http.StatusInternalServerError))
		return
	}

	logError(writeJSON(w, versions, http.StatusOK))
}
