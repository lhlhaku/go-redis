package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// FlushDB removes all data in current database
func FlushDB(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {

	//遇到一个错误就结束
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return &reply.OkReply{}
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
