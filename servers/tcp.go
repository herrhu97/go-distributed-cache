package servers

import (
	"cache-server/caches"
	"cache-server/helpers"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/FishGoddess/vex"
)

const (
	getCommand = byte(1)

	setCommand = byte(2)

	deleteCommand = byte(3)

	statusCommand = byte(4)

	nodesCommand = byte(5)
)

var (
	errCommandNeedsMoreArguments = errors.New("command needs more arguments")

	errNotFound = errors.New("not found")
)

// TCPServer 是TCP类型的服务器
// TCPClient 理应做并发安全的保证也没做，所以这个客户端在并发下是有问题
type TCPServer struct {
	*node

	// cache 是内部用于存储数据的缓存组件。
	cache *caches.Cache

	// server 是内部真正用于服务的服务器。
	server *vex.Server

	options *Options
}

// NewTCPServer 返回新的TCP服务器
func NewTCPServer(cache *caches.Cache, options *Options) (*TCPServer, error) {
	n, err := newNode(options)
	if err != nil {
		return nil, err
	}

	return &TCPServer{
		node:    n,
		cache:   cache,
		server:  vex.NewServer(),
		options: options,
	}, nil
}

// Run 运行这个TCP服务器
func (ts *TCPServer) Run() error {
	ts.server.RegisterHandler(getCommand, ts.getHandler)
	ts.server.RegisterHandler(setCommand, ts.setHandler)
	ts.server.RegisterHandler(deleteCommand, ts.deleteHandler)
	ts.server.RegisterHandler(statusCommand, ts.statusHandler)

	ts.server.RegisterHandler(nodesCommand, ts.nodesHandler)
	return ts.server.ListenAndServe("tcp", helpers.JoinAddressAndPort(ts.options.Address, ts.options.Port))
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

	// 使用一致性哈希选择出这个 key 所属的物理节点
	key := string(args[0])
	node, err := ts.selectNode(key)
	if err != nil {
		return nil, err
	}

	// 判断这个 key 所属的物理节点是否是当前节点，如果不是，需要响应重定向信息给客户端，并告知正确的节点地址
	if !ts.isCurrentNode(node) {
		return nil, fmt.Errorf("redirect to node %s", node)
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

	// 使用一致性哈希选择出这个 key 所属的物理节点
    key := string(args[1])
    node, err := ts.selectNode(key)
    if err != nil {
        return nil, err
    }

    // 判断这个 key 所属的物理节点是否是当前节点，如果不是，需要响应重定向信息给客户端，并告知正确的节点地址
    if !ts.isCurrentNode(node) {
        return nil, fmt.Errorf("redirect to node %s", node)
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

	// 使用一致性哈希选择出这个 key 所属的物理节点
    key := string(args[0])
    node, err := ts.selectNode(key)
    if err != nil {
        return nil, err
    }

    // 判断这个 key 所属的物理节点是否是当前节点，如果不是，需要响应重定向信息给客户端，并告知正确的节点地址
    if !ts.isCurrentNode(node) {
        return nil, fmt.Errorf("redirect to node %s", node)
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

// nodesHandler 是返回集群所有节点名称的处理器。
func (ts *TCPServer) nodesHandler(args [][]byte) (body []byte, err error) {
	return json.Marshal(ts.nodes())
}
