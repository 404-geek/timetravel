package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/api"
	"github.com/rainbowmga/timetravel/service"
)

// logError logs all non-nil errors
func logError(err error) {
	if err != nil {
		log.Printf("error: %v", err)
	}
}

func main() {
	router := mux.NewRouter()

	svc, err := service.NewSQLiteRecordService("records.db")
	if err != nil {
		log.Fatalf("failed to create record service: %v", err)
	}
	defer svc.Close()

	// V1 API — behaviour unchanged for backward compatibility.
	v1API := api.NewAPI(svc)
	v1Route := router.PathPrefix("/api/v1").Subrouter()
	v1Route.Path("/health").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		logError(err)
	})
	v1API.CreateRoutes(v1Route)

	// V2 API — time-travel versioning.
	v2API := api.NewV2API(svc)
	v2Route := router.PathPrefix("/api/v2").Subrouter()
	v2API.CreateRoutes(v2Route)

	address := "127.0.0.1:8000"
	srv := &http.Server{
		Handler:      router,
		Addr:         address,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Printf("listening on %s", address)
	log.Fatal(srv.ListenAndServe())
}
