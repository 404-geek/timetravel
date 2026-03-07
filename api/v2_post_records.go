package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/entity"
	"github.com/rainbowmga/timetravel/service"
)

// POST /api/v2/records/{id}
// If the record exists, it is updated (a new version is created).
// If the record does not exist, it is created as version 1.
// Returns the resulting VersionedRecord.
func (a *V2API) PostRecords(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := mux.Vars(r)["id"]
	idNumber, err := strconv.ParseInt(id, 10, 32)

	if err != nil || idNumber <= 0 {
		logError(writeError(w, "invalid id; id must be a positive number", http.StatusBadRequest))
		return
	}

	var body map[string]*string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		logError(writeError(w, "invalid input; could not parse json", http.StatusBadRequest))
		return
	}

	// Attempt to update an existing record; fall back to create if it does not exist.
	_, err = a.records.GetRecord(ctx, int(idNumber))
	if !errors.Is(err, service.ErrRecordDoesNotExist) {
		_, err = a.records.UpdateRecord(ctx, int(idNumber), body)
	} else {
		// Exclude null-value keys on initial creation.
		recordMap := map[string]string{}
		for key, value := range body {
			if value != nil {
				recordMap[key] = *value
			}
		}
		err = a.records.CreateRecord(ctx, entity.Record{
			ID:   int(idNumber),
			Data: recordMap,
		})
	}

	if err != nil {
		logError(err)
		logError(writeError(w, ErrInternal.Error(), http.StatusInternalServerError))
		return
	}

	// Return the newly created/updated versioned record.
	vRecord, err := a.records.GetVersionedRecord(ctx, int(idNumber), 0)
	if err != nil {
		logError(err)
		logError(writeError(w, ErrInternal.Error(), http.StatusInternalServerError))
		return
	}

	logError(writeJSON(w, vRecord, http.StatusOK))
}
