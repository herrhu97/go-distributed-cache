package servers

import (
	"cache-server/caches"
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/FishGoddess/vex"
)

const (
	getCommand = byte(1)

	setCommand = byte(2)

	deleteCommand = byte(3)

	statusCommand = byte(4)
)

var (
	errCommandNeedsMoreArguments = errors.New("command needs more arguments")

	errNotFound = errors.New("not found")
)

// TCPServer 是TCP类型的服务器
// TCPClient 理应做并发安全的保证也没做，所以这个客户端在并发下是有问题
type TCPServer struct {
	// cache 是内部用于存储数据的缓存组件。
	cache *caches.Cache

	// server 是内部真正用于服务的服务器。
	server *vex.Server
}

// NewTCPServer 返回新的TCP服务器
func NewTCPServer(cache *caches.Cache) *TCPServer {
	return &TCPServer{
		cache:  cache,
		server: vex.NewServer(),
	}
}

// Run 运行这个TCP服务器
func (ts *TCPServer) Run(address string) error {
	ts.server.RegisterHandler(getCommand, ts.getHandler)
	ts.server.RegisterHandler(setCommand, ts.setHandler)
	ts.server.RegisterHandler(deleteCommand, ts.deleteHandler)
	ts.server.RegisterHandler(statusCommand, ts.statusHandler)
	return ts.server.ListenAndServe("tcp", address)
}

// Close 用于关闭服务器
func (ts *TCPServer) Close() error {
	return ts.server.Close()
}

// =======================================================================

// getHandler 是处理 get 命令的的处理器。
func (ts *TCPServer) getHandler(args [][]byte) (body []byte, err error) {
	// 检查参数字数是否足够
	if len(args) < 1 {
		return nil, errCommandNeedsMoreArguments
	}

	// 调用缓存的Get方法，如果不存在就返回noFoundErr错误
	value, ok := ts.cache.Get(string(args[0]))
	if !ok {
		return value, errNotFound
	}
	return value, nil
}

// setHandler 是处理set命令的处理器
func (ts *TCPServer) setHandler(args [][]byte) (body []byte, err error) {
	// 检查参数个数是否足够
	if len(args) < 3 {
		return nil, errCommandNeedsMoreArguments
	}

	// 读取ttl，注意这里使用大端的方式读取，所以要求客户端也以大端的方式进行存储
	ttl := int64(binary.BigEndian.Uint64(args[0]))
	err = ts.cache.SetWithTTL(string(args[1]), args[2], ttl)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// deleteHandler 是处理delete命令的处理器
func (ts *TCPServer) deleteHandler(args [][]byte) (body []byte, err error) {
	// 检查参数个数是否足够
	if len(args) < 1 {
		return nil, errCommandNeedsMoreArguments
	}

	// 删除指定的数据
	err = ts.cache.Delete(string(args[0]))
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// statusHandler 是返回缓存状态的处理器
func (ts *TCPServer) statusHandler(args [][]byte) (body []byte, err error) {
	return json.Marshal(ts.cache.Status())
}


