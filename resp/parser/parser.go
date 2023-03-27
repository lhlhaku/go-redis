package parser

import (
	"bufio"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

//简单字符串：以"+" 开始， 如："+OK\r\n"
//错误：以"-" 开始，如："-ERR Invalid Synatx\r\n"
//整数：以":"开始，如：":1\r\n"
//字符串：以 $ 开始
//数组：以 * 开始
/* 把客户的发来的信息进行解析*/

// Payload stores redis.Reply or error
type Payload struct {
	Data resp.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
// 把io.Reader穿过来的数据先解析，然后返回给你一个管道，把解析的结果送入管道
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// 解析器的状态
type readState struct {
	readingMultiLine  bool     //解析多行还是单行
	expectedArgsCount int      //期望长度
	msgType           byte     //用户消息类型
	args              [][]byte // 用户发来的消息的解析
	bulkLen           int64    //期望收到字符串的长度
}

//判断解析器是否以及完成
func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

//让redis的协议解析和核心业务分开并发的跑
//边解析边进行核心业务
//核心  解析器
//为什么io.Reader？因为tcp下层传给服务器的就是通过io.Reader传过来的

//parse0是个死循环，除非链接中断

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil { // 考虑到程序执行过程中可能出现panic，所以我们需要recover()
			//防止出现for循环里面出现panic退出来
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// read line
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state) //读进来一行数据

		if err != nil {
			if ioErr { // encounter io err, stop read 如果是io err 直接放到管道里，解析任务结束
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// protocol err, reset read state  如果是协议错误，返回错误并重置解析器的状态
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// parse line 判断是否是多行解析模式
		if !state.readingMultiLine { //非多行（多行解析还没开始）（就是当行输入）
			// receive new response
			if msg[0] == '*' {
				// multi bulk reply
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else if msg[0] == '$' { // bulk reply
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.bulkLen == -1 { // null bulk reply //-1表示空的 //$-1\r\n
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else {
				// single line reply
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{} // reset state
				continue
			}
			//*3
			//$3
			//SET
			//$3
			//key
			//$5
			//value
			// *3\r\n $3\r\n SET\r\n $3\r\n key\r\n $5\r\n value\r\n
		} else { // 多头后续接受数据  如果是多头跳过parseBulkHeader 直接readBody 并且每个Bulk要读的长度由BUlKLen决定
			//如果是两行的bulk string 执行parseBulkHeader，需要expectedArgsCount为1
			// receive following bulk reply
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// if sending finished
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// readLine 从io.read里取数据，一行一行的取，以\n为结尾符号
// bool表示是否是io错误
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	// 1. 没有读到$,以\r\n切分

	if state.bulkLen == 0 { // 1. \r\n切分 此时以\n切分
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' { //要么消息len为0,要么读的不对，倒数第二个不是\r
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		// 2. 之前读到了$数字，严格读取字符个数
	} else { // read bulk line (binary safe)
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err //有io错误
		}
		if len(msg) == 0 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

//*3
//$3
//SET
//$3
//key
//$5
//value
// *3\r\n $3\r\n SET\r\n $3\r\n key\r\n $5\r\n value\r\n
//对Bulk头进行解析

//// 解析器的状态
//type readState struct {
//	readingMultiLine  bool     //解析多行还是单行
//	expectedArgsCount int      //期望长度
//	msgType           byte     //用户消息类型
//	args              [][]byte // 用户发来的消息的解析
//	bulkLen           int64    //整个字节的长度
//}
// 数组(Array, 又称 Multi Bulk Strings): Bulk String 数组，客户端发送指令以及 lrange 等命令响应的格式
/// 以*开始  后面都是parseBulkHeader
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32) //  *3\r\n 得到3  我们得到命令数组的长度
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// first line of multi bulk reply
		state.msgType = msg[0]                       // 把*赋予msgType，表示一个array
		state.readingMultiLine = true                //是个多行
		state.expectedArgsCount = int(expectedLine)  //期望长度为3
		state.args = make([][]byte, 0, expectedLine) // set key value 有三个字符串
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

//解析字符串 Bulk String 有两行，
//第一行为$+正文长度
//第二行为实际内容，并且可以包含\r\n
//  $4
//  a\r\nb
// $$$$$$$$$

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// +ok\r\n 简单字符串表示状态  -err\r\n 错误表示错误 :5\r\n  整数 这三种  一个简单单行解析
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+': // status reply
		result = reply.MakeStatusReply(str[1:])
	case '-': // err reply
		result = reply.MakeErrReply(str[1:])
	case ':': // int reply
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

// read the non-first lines of multi bulk reply or bulk reply
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2] //切去\r\n
	var err error
	if line[0] == '$' {
		// bulk reply
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
