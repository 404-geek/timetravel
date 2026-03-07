package api

import (
	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/service"
)

type V2API struct {
	records service.VersionedRecordService
}

func NewV2API(records service.VersionedRecordService) *V2API {
	return &V2API{records}
}

// CreateRoutes registers the v2 routes onto the provided router.
func (a *V2API) CreateRoutes(routes *mux.Router) {
	routes.Path("/records/{id}/versions").HandlerFunc(a.GetRecordVersions).Methods("GET")
	routes.Path("/records/{id}").HandlerFunc(a.GetRecords).Methods("GET")
	routes.Path("/records/{id}").HandlerFunc(a.PostRecords).Methods("POST")
}
