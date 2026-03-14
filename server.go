package main

import (
	"encoding/json"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/api"
	"github.com/rainbowmga/timetravel/service"
	"github.com/rainbowmga/timetravel/db"
)

// logError logs all non-nil errors
func logError(err error) {
	if err != nil {
		log.Printf("error: %v", err)
	}
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &responseRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		result := "PASS"
		if rec.status >= 400 {
			result = "FAIL"
		}
		log.Printf("%s %s → %d %s", r.Method, r.URL.Path, rec.status, result)
	})
}

func main() {
	router := mux.NewRouter()
	dbPath := filepath.Join("db", "data", "records.db")
	sqliteDB, err := db.OpenDB(dbPath)
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}
	defer sqliteDB.Close()

	recordService := service.NewSQLiteRecordService(sqliteDB)
	api := api.NewAPI(&recordService)

	apiRoute := router.PathPrefix("/api/v1").Subrouter()
	apiRoute.Path("/health").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		logError(err)
	})
	api.CreateRoutes(apiRoute)
	handler := requestLog(router)

	address := "127.0.0.1:8000"
	srv := &http.Server{
		Handler:      handler,
		Addr:         address,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Printf("listening on %s", address)
	log.Fatal(srv.ListenAndServe())
}
