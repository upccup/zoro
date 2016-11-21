package store

type Store interface {
	PutKeyValue(key string, value []byte) error

	GetKeyValue(key string) (string, error)
}
