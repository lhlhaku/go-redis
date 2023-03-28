package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// Rename renames a key, the origin and the destination must within the same node
// 没有实现rename在不同节点
// 修改玩key后，新的Key的hash按照要求会在不同的节点
// 暂时没有实现，如果新的Key在不同节点就不修改报错，否则就修改
func Rename(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])
	dest := string(args[2])

	srcPeer := cluster.peerPicker.PickNode(src)
	destPeer := cluster.peerPicker.PickNode(dest)

	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	return cluster.relay(srcPeer, c, args)
}
