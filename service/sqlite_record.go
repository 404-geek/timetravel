package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rainbowmga/timetravel/entity"
	_ "modernc.org/sqlite"
)

// SQLiteRecordService is a SQLite-backed implementation of VersionedRecordService.
// Every create or update produces a new version row, giving a complete audit trail.
type SQLiteRecordService struct {
	db *sql.DB
}

// NewSQLiteRecordService opens (or creates) the SQLite database at dbPath and
// runs any necessary schema migrations. Use ":memory:" for an in-memory database.
func NewSQLiteRecordService(dbPath string) (*SQLiteRecordService, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	svc := &SQLiteRecordService{db: db}
	if err := svc.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate database: %w", err)
	}

	return svc, nil
}

// Close closes the underlying database connection.
func (s *SQLiteRecordService) Close() error {
	return s.db.Close()
}

// migrate creates the necessary tables if they do not already exist.
func (s *SQLiteRecordService) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS record_versions (
			id         INTEGER NOT NULL,
			version    INTEGER NOT NULL,
			data       TEXT    NOT NULL,
			created_at TEXT    NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (id, version)
		)
	`)
	return err
}

// --- RecordService implementation ---

func (s *SQLiteRecordService) GetRecord(ctx context.Context, id int) (entity.Record, error) {
	vr, err := s.GetVersionedRecord(ctx, id, 0)
	if err != nil {
		return entity.Record{}, err
	}
	return entity.Record{ID: vr.ID, Data: vr.Data}, nil
}

func (s *SQLiteRecordService) CreateRecord(ctx context.Context, record entity.Record) error {
	if record.ID <= 0 {
		return ErrRecordIDInvalid
	}

	data, err := json.Marshal(record.Data)
	if err != nil {
		return fmt.Errorf("marshal record data: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO record_versions (id, version, data) VALUES (?, 1, ?)`,
		record.ID, string(data),
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ErrRecordAlreadyExists
		}
		return fmt.Errorf("insert record: %w", err)
	}

	return nil
}

func (s *SQLiteRecordService) UpdateRecord(ctx context.Context, id int, updates map[string]*string) (entity.Record, error) {
	if id <= 0 {
		return entity.Record{}, ErrRecordIDInvalid
	}

	// Use a transaction so the read-modify-write is atomic.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return entity.Record{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Fetch the current (latest) record data and version.
	var (
		currentDataStr string
		maxVersion     int
	)
	err = tx.QueryRowContext(ctx,
		`SELECT data, version FROM record_versions WHERE id = ? ORDER BY version DESC LIMIT 1`,
		id,
	).Scan(&currentDataStr, &maxVersion)
	if err == sql.ErrNoRows {
		return entity.Record{}, ErrRecordDoesNotExist
	}
	if err != nil {
		return entity.Record{}, fmt.Errorf("query current record: %w", err)
	}

	// Decode current data and apply the updates.
	currentData := map[string]string{}
	if err := json.Unmarshal([]byte(currentDataStr), &currentData); err != nil {
		return entity.Record{}, fmt.Errorf("unmarshal record data: %w", err)
	}
	for key, value := range updates {
		if value == nil {
			delete(currentData, key)
		} else {
			currentData[key] = *value
		}
	}

	// Persist as a new version.
	newData, err := json.Marshal(currentData)
	if err != nil {
		return entity.Record{}, fmt.Errorf("marshal updated data: %w", err)
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO record_versions (id, version, data) VALUES (?, ?, ?)`,
		id, maxVersion+1, string(newData),
	)
	if err != nil {
		return entity.Record{}, fmt.Errorf("insert new version: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return entity.Record{}, fmt.Errorf("commit transaction: %w", err)
	}

	return entity.Record{ID: id, Data: currentData}, nil
}

// --- VersionedRecordService implementation ---

// scanVersionedRecord scans a single row into a VersionedRecord.
func scanVersionedRecord(recordID, ver int, dataStr, createdAtStr string) (entity.VersionedRecord, error) {
	createdAt, err := time.Parse("2006-01-02 15:04:05", createdAtStr)
	if err != nil {
		return entity.VersionedRecord{}, fmt.Errorf("parse created_at: %w", err)
	}

	var data map[string]string
	if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
		return entity.VersionedRecord{}, fmt.Errorf("unmarshal record data: %w", err)
	}

	return entity.VersionedRecord{
		ID:        recordID,
		Version:   ver,
		Data:      data,
		CreatedAt: createdAt.UTC(),
	}, nil
}

// GetVersionedRecord returns the record at the given version.
// If version <= 0, the latest version is returned.
func (s *SQLiteRecordService) GetVersionedRecord(ctx context.Context, id int, version int) (entity.VersionedRecord, error) {
	if id <= 0 {
		return entity.VersionedRecord{}, ErrRecordIDInvalid
	}

	var (
		query string
		args  []interface{}
	)
	if version <= 0 {
		query = `SELECT id, version, data, created_at FROM record_versions WHERE id = ? ORDER BY version DESC LIMIT 1`
		args = []interface{}{id}
	} else {
		query = `SELECT id, version, data, created_at FROM record_versions WHERE id = ? AND version = ?`
		args = []interface{}{id, version}
	}

	var (
		recordID     int
		ver          int
		dataStr      string
		createdAtStr string
	)
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&recordID, &ver, &dataStr, &createdAtStr)
	if err == sql.ErrNoRows {
		if version > 0 {
			return entity.VersionedRecord{}, ErrRecordVersionDoesNotExist
		}
		return entity.VersionedRecord{}, ErrRecordDoesNotExist
	}
	if err != nil {
		return entity.VersionedRecord{}, fmt.Errorf("query record: %w", err)
	}

	return scanVersionedRecord(recordID, ver, dataStr, createdAtStr)
}

// ListRecordVersions returns all versions of the record with the given id,
// ordered from the oldest (version 1) to the newest.
func (s *SQLiteRecordService) ListRecordVersions(ctx context.Context, id int) ([]entity.VersionedRecord, error) {
	if id <= 0 {
		return nil, ErrRecordIDInvalid
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, version, data, created_at FROM record_versions WHERE id = ? ORDER BY version ASC`,
		id,
	)
	if err != nil {
		return nil, fmt.Errorf("query record versions: %w", err)
	}
	defer rows.Close()

	var versions []entity.VersionedRecord
	for rows.Next() {
		var (
			recordID     int
			ver          int
			dataStr      string
			createdAtStr string
		)
		if err := rows.Scan(&recordID, &ver, &dataStr, &createdAtStr); err != nil {
			return nil, fmt.Errorf("scan record version: %w", err)
		}

		vr, err := scanVersionedRecord(recordID, ver, dataStr, createdAtStr)
		if err != nil {
			return nil, err
		}
		versions = append(versions, vr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate record versions: %w", err)
	}

	if len(versions) == 0 {
		return nil, ErrRecordDoesNotExist
	}

	return versions, nil
}
