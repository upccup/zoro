package store

type Store interface {
	PutKeyValue(key, value string) error

	GetKeyValue(key string) (string, error)
}
