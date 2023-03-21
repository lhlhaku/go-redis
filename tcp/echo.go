package tcp

import (
	"bufio"
	"context"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// EchoClient go原生的waitGroup没有超时等待,这里用我们自己定义的
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// Close 关闭的统一接口
// 一个客户端要关闭conn 也就是socket 关闭，但是关闭前需要保证业务已经完成
func (e *EchoClient) Close() error {
	//TODO implement me
	e.Waiting.WaitWithTimeout(10 * time.Second)
	_ = e.Conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	//TODO implement me
	if handler.closing.Get() {
		_ = conn.Close()
	}
	client := &EchoClient{Conn: conn}
	handler.activeConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)
	for true {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("Connection CLose")
				handler.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.Waiting.Done()
	}
}

func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down")
	handler.closing.Set(true)
	handler.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Conn.Close()
		return true //true表示操作下一个，false不操作下一个
	})
	return nil
}
