package resp

// Connection 代表一个客户端的连接
type Connection interface {
	Write([]byte) error

	GetDBIndex() int
	SelectDB(int) //切换库

}
