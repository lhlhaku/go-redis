package resp

// Reply 回复给客户端
type Reply interface {
	ToBytes() []byte //tcp基于字节的，所以需要全部转换成字节
}
