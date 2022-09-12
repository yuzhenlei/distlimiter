package remotestore

type MySQLAdaptor struct {

}

func NewMySQL() *MySQLAdaptor {
	return &MySQLAdaptor{}
}

func (mysql *MySQLAdaptor) Send(entry string) error {
	return nil
}

func (mysql *MySQLAdaptor) Pull() ([]string, error) {
	return nil, nil
}