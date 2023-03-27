package database

import (
	"go-redis/interface/resp"
)

//代表redis的业务核心

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte //二维字节组

// Database is the interface for redis style storage engine
type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply //执行指令
	AfterClientClose(c resp.Connection)                    //关闭后的善后工作
	Close()
}

// DataEntity stores data bound to a key, including a string, list, hash, set and so on
type DataEntity struct { //指代redis数据结构
	Data interface{}
}
