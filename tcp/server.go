package tcp

import (
	"context"
	"fmt"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Config stores tcp server properties
type Config struct {
	Address string
}

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
// 创建一个os lever chan和子协程监听系统是否发来关闭信号，如果发来信号，则向closeChan写入空结构体，通知关闭
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close

// ListenAndServe 两个逻辑：一个是正常关闭，二是我们手动关闭进程，导致无法执行到defer中释放资源。
// 一是正常关闭，需要考虑到 连接到坏请求导致退出，但是好请求还没有处理完毕，所以需要等待它们执行结束，加个waitGroup
// 二是特殊关闭，所以创建一个协程 一直读取管道，如果管道发来空接口体（信号）就执行关闭，并释放资源，信号由ListenAndServeWithSignal发送
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	go func() {
		<-closeChan
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	// listen port
	defer func() {
		// close during unexpected error
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()

	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		// handle
		logger.Info("accept link")
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait() // 加等待队列的意义：遇到错误链接Break后，需要把其他协程处理函数处理完后main才能退出

}
