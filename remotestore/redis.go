package remotestore

import (
	"fmt"
	redisclient "github.com/gomodule/redigo/redis"
	"time"
)

type RedisAdaptor struct {
	conn redisclient.Conn
	key  string
}

type RedisOptions struct {
	Addr string
	Key  string
}

func NewRedis(options *RedisOptions) *RedisAdaptor {
	if options.Addr == "" {
		panic("redis addr must specified")
	}
	if options.Key == "" {
		panic("redis key must specified")
	}
	return &RedisAdaptor{
		key: options.Key,
	}
}

func (redis *RedisAdaptor) getConn() (redisclient.Conn, error) {
	if redis.conn != nil {
		return redis.conn, nil
	}
	conn, err := redisclient.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		return nil, fmt.Errorf("redis connect fail")
	}
	redis.conn = conn
	return conn, nil
}

func (redis *RedisAdaptor) Send(now time.Time, entry string) error {
	conn, err := redis.getConn()
	if err != nil {
		return fmt.Errorf("get redis conn fail: %s", err.Error())
	}
	_, err = conn.Do("zadd", redis.key, now.Unix(), entry)
	if err != nil {
		return fmt.Errorf("zadd error: %s", err.Error())
	}
	return nil
}

func (redis *RedisAdaptor) Pull(min time.Time, max time.Time) ([]string, error) {
	conn, err := redis.getConn()
	if err != nil {
		return nil, err
	}
	ids, err := redisclient.Strings(conn.Do("zrangebyscore", redis.key, min.Unix(), max.Unix()))
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore error: %s", err.Error())
	}
	return ids, nil
}

func (redis *RedisAdaptor) Clear(until time.Time) error {
	return nil
}
