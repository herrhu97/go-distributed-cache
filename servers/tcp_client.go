package servers

import (
	"cache-server/caches"
	"encoding/binary"
	"encoding/json"

	"github.com/FishGoddess/vex"
)

// TCPClient 是TCP客户端结构
type TCPClient struct {
	client *vex.Client
}

// NewTCPClient 返回一个新的TCP客户端
func NewTCPClient(address string) (*TCPClient, error) {
	client, err := vex.NewClient("tcp", address)
	if err != nil {
		return nil, err
	}
	return &TCPClient{client: client}, nil
}

// Get 获取指定key的value
func (tc *TCPClient) Get(key string) ([]byte, error) {
	return tc.client.Do(getCommand, [][]byte{
		[]byte(key),
	})
}

// Set 添加一个键值对到缓存中
func (tc *TCPClient) Set(key string, value []byte, ttl int64) error {
	// 用大端的形式存储数字
	ttlBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBytes, uint64(ttl))
	_, err := tc.client.Do(setCommand, [][]byte{
		ttlBytes, []byte(key), value,
	})

	return err
}

// Delete 删除指定key的value
func (tc *TCPClient) Delete(key string) error {
	_, err := tc.client.Do(deleteCommand, [][]byte{
		[]byte(key),
	})
	return err
}

// Status 返回缓存的状态
func (tc *TCPClient) Status() (*caches.Status, error) {
	body, err := tc.client.Do(statusCommand, nil)
	if err != nil {
		return nil, err
	}

	status := caches.NewStatus()
	err = json.Unmarshal(body, status)
	return status, err
}

// Nodes 返回集群中的所有节点名称。
func (tc *TCPClient) Nodes() ([]string, error) {
    body, err := tc.client.Do(nodesCommand, nil)
    if err != nil {
        return nil, err
    }
    var nodes []string
    err = json.Unmarshal(body, &nodes)
    return nodes, err
}

// Close 关闭这个客户端
func (tc *TCPClient) Close() error {
	return tc.client.Close()
}
