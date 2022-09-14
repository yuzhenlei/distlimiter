package remotestore

import (
	"fmt"
	redisclient "github.com/gomodule/redigo/redis"
	"time"
)

type RedisAdaptor struct {
	conn redisclient.Conn
	key string
}

func NewRedis(key string) *RedisAdaptor {
	conn, err := redisclient.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		panic(fmt.Sprintf("redis connect fail: %s", err.Error()))
	}
	return &RedisAdaptor{
		conn: conn,
		key: key,
	}
}

func (redis *RedisAdaptor) Send(now time.Time, entry string) error {
	_, err := redis.conn.Do("zadd", redis.key, now.Unix(), entry)
	if err != nil {
		return fmt.Errorf("zadd error: %s", err.Error())
	}
	return nil
}

func (redis *RedisAdaptor) Pull(min time.Time, max time.Time) ([]string, error) {
	ids, err := redisclient.Strings(redis.conn.Do("zrangebyscore", redis.key, min.Unix(), max.Unix()))
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore error: %s", err.Error())
	}
	return ids, nil
}