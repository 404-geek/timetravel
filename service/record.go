package service

import (
	"context"
	"errors"
	"database/sql"
	"encoding/json"

	"github.com/rainbowmga/timetravel/entity"
)

var ErrRecordDoesNotExist = errors.New("record with that id does not exist")
var ErrRecordIDInvalid = errors.New("record id must >= 0")
var ErrRecordAlreadyExists = errors.New("record already exists")

// Implements method to get, create, and update record data.
type RecordService interface {

	// GetRecord will retrieve an record.
	GetRecord(ctx context.Context, id int) (entity.Record, error)

	// CreateRecord will insert a new record.
	//
	// If it a record with that id already exists it will fail.
	CreateRecord(ctx context.Context, record entity.Record) error

	// UpdateRecord will change the internal `Map` values of the record if they exist.
	// if the update[key] is null it will delete that key from the record's Map.
	//
	// UpdateRecord will error if id <= 0 or the record does not exist with that id.
	UpdateRecord(ctx context.Context, id int, updates map[string]*string) (entity.Record, error)
}

// InMemoryRecordService is an in-memory implementation of RecordService.
type InMemoryRecordService struct {
	data map[int]entity.Record
}

type SQLiteRecordService struct {
	db *sql.DB
}

func NewSQLiteRecordService(db *sql.DB) SQLiteRecordService {
	return SQLiteRecordService{
		db: db,
	}
}

func (s *SQLiteRecordService) GetRecord(ctx context.Context, id int) (entity.Record, error) {
	if id <= 0 {
		return entity.Record{}, ErrRecordIDInvalid
	}
	var dataJSON string
	err := s.db.QueryRowContext(ctx, `SELECT data FROM records WHERE id = ?`, id).Scan(&dataJSON)
	if err == sql.ErrNoRows {
		return entity.Record{}, ErrRecordDoesNotExist
	}
	if err != nil {
		return entity.Record{}, err
	}
	var data map[string]string
	if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
		return entity.Record{}, err
	}
	return entity.Record{ID: id, Data: data}, nil
}


func (s *SQLiteRecordService) CreateRecord(ctx context.Context, record entity.Record) error {
	if record.ID <= 0 {
		return ErrRecordIDInvalid
	}
	dataJSON, err := json.Marshal(record.Data)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO records (id, data) VALUES (?, ?)`, record.ID, string(dataJSON))
	if err != nil {
		return ErrRecordAlreadyExists
	}
	return nil
}

func (s *SQLiteRecordService) UpdateRecord(ctx context.Context, id int, updates map[string]*string) (entity.Record, error) {
	record, err := s.GetRecord(ctx, id)
	if err != nil {
		return entity.Record{}, err
	}
	for key, value := range updates {
		if value == nil {
			delete(record.Data, key)
		} else {
			record.Data[key] = *value
		}
	}
	dataJSON, err := json.Marshal(record.Data)
	if err != nil {
		return entity.Record{}, err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE records SET data = ? WHERE id = ?`, string(dataJSON), id)
	if err != nil {
		return entity.Record{}, err
	}
	return record, nil
}

func NewInMemoryRecordService() InMemoryRecordService {
	return InMemoryRecordService{
		data: map[int]entity.Record{},
	}
}

func (s *InMemoryRecordService) GetRecord(ctx context.Context, id int) (entity.Record, error) {
	record := s.data[id]
	if record.ID == 0 {
		return entity.Record{}, ErrRecordDoesNotExist
	}

	record = record.Copy() // copy is necessary so modifations to the record don't change the stored record
	return record, nil
}

func (s *InMemoryRecordService) CreateRecord(ctx context.Context, record entity.Record) error {
	id := record.ID
	if id <= 0 {
		return ErrRecordIDInvalid
	}

	existingRecord := s.data[id]
	if existingRecord.ID != 0 {
		return ErrRecordAlreadyExists
	}

	s.data[id] = record
	return nil
}

func (s *InMemoryRecordService) UpdateRecord(ctx context.Context, id int, updates map[string]*string) (entity.Record, error) {
	entry := s.data[id]
	if entry.ID == 0 {
		return entity.Record{}, ErrRecordDoesNotExist
	}

	for key, value := range updates {
		if value == nil { // deletion update
			delete(entry.Data, key)
		} else {
			entry.Data[key] = *value
		}
	}

	return entry.Copy(), nil
}
