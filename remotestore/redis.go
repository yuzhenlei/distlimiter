package remotestore

type RedisAdaptor struct {
	key string
}

func NewRedis(key string) *RedisAdaptor {
	return &RedisAdaptor{key: key}
}

func (redis *RedisAdaptor) Send(entry string) error {
	return nil
}

func (redis *RedisAdaptor) Pull() ([]string, error) {
	return nil, nil
}