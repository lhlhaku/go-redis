package database

import (
	"fmt"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

// Database is a set of multiple database set
type Database struct {
	dbSet []*DB
}

// NewDatabase creates a redis database,
func NewDatabase() *Database {
	mdb := &Database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	return mdb
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
// set k v         get k        select 2  等等很多命令 只有select 整个命令在此层做
func (mdb *Database) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) { //用户选的db， 用户发的命令都交给connection里记录的Db去执行
	defer func() { //防止整个协程都崩溃
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}
	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// Close graceful shutdown database
func (mdb *Database) Close() { // 没有什么特殊的逻辑

}

func (mdb *Database) AfterClientClose(c resp.Connection) { // 没有什么特殊的逻辑
}

// select 2
func execSelect(c resp.Connection, mdb *Database, args [][]byte) resp.Reply { //用户切换db的逻辑  用户要选到该db   // 用户connection 里记录者此时用到的库
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
