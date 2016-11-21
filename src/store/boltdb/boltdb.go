package boltdb

import (
	"errors"

	"github.com/boltdb/bolt"
)

type Boltdb struct {
	*bolt.DB
}

var (
	bucketKeyStorageVersion = []byte("v1")
)

var (
	errStorageVersionUnknown = errors.New("boltdb: storage version unknown")
)

func NewBoltdbStore(db *bolt.DB) (*Boltdb, error) {
	if err := db.Update(func(tx *bolt.Tx) {
		createBucketIfNotExists(tx, bucketKeyStorageVersion)
	}); err != nil {
		return nil, err
	}

	return &Boltdb{
		DB: db,
	}, nil
}

func createBucketIfNotExists(tx *bolt.Tx, keys ...[]byte) (*bolt.Bucket, error) {
	bkt, err := tx.CreateBucketIfNotExists(keys[0])
	if err != nil {
		return nil, err
	}

	for _, key := range keys[1:] {
		bkt, err = tx.CreateBucketIfNotExists(key)
		if err != nil {
			return nil, err
		}
	}

	return bkt, nil
}

func (db *Boltdb) PutKeyValue(key string, value []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketKeyStorageVersion)

		if bkt == nil {
			return errStorageVersionUnknown
		}

		return bkt.Put([]byte(key), value)
	})
}

func (db *Boltdb) GetKeyValue(key string) (string, error) {
	var value string

	if err := db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketKeyStorageVersion)
		if bkt == nil {
			return errStorageVersionUnknown
		}

		value = string(bkt.Get([]byte(key)))
		return nil
	}); err != nil {
		return value, err
	}

	return value, nil
}
