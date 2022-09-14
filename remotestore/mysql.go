package remotestore

import "time"

type MySQLAdaptor struct {

}

func NewMySQL() *MySQLAdaptor {
	return &MySQLAdaptor{}
}

func (mysql *MySQLAdaptor) Send(now time.Time, entry string) error {
	return nil
}

func (mysql *MySQLAdaptor) Pull(min time.Time, max time.Time) ([]string, error) {
	return nil, nil
}