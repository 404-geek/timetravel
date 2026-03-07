package entity

import "time"

type Record struct {
	ID   int               `json:"id"`
	Data map[string]string `json:"data"`
}

func (d *Record) Copy() Record {
	values := d.Data

	newMap := map[string]string{}
	for key, value := range values {
		newMap[key] = value
	}

	return Record{
		ID:   d.ID,
		Data: newMap,
	}
}

// VersionedRecord represents a record at a specific point in time.
type VersionedRecord struct {
	ID        int               `json:"id"`
	Version   int               `json:"version"`
	Data      map[string]string `json:"data"`
	CreatedAt time.Time         `json:"created_at"`
}
