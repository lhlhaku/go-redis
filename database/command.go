package database

import (
	"strings"
)

var cmdTable = make(map[string]*command) //每个指令 都是一个结构体对应

// 每个命令都是一个command结构体，里面有一个命令的实现方法
type command struct {
	executor ExecFunc
	arity    int // allow number of args, arity < 0 means len(args) >= -arity  参数的数量 set,put的参数数量不一样
}

// RegisterCommand registers a new command
// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
// for example: the arity of `get` is 2, `mget` is -2
func RegisterCommand(name string, executor ExecFunc, arity int) { //注册对应的命令
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		arity:    arity,
	}
}
