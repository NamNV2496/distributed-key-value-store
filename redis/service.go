package redis

import "context"

// RedisServiceServer is the server interface for Redis service
type RedisServiceServer interface {
	Set(context.Context, *SetRequest) (*Empty, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Delete(context.Context, *DeleteRequest) (*Empty, error)
}

// Message types for Redis service
type SetRequest struct {
	Key      string
	Value    []byte
	ExpireMs int64
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value []byte
	Found bool
}

type DeleteRequest struct {
	Key string
}

type Empty struct{}
