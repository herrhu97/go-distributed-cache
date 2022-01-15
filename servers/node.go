package servers

import (
	"cache-server/helpers"
	"io/ioutil"
	"time"

	"github.com/hashicorp/memberlist"
	"stathat.com/c/consistent"
)

// node 代表集群中的一个节点，会保存一些和集群相关的数据。
type node struct {
	// options 存储着一些服务器相关的选项。
	options *Options

	// address 记录的是当前节点的访问地址，包含 ip 或者主机、端口等信息。
	address string

	// circle 是一致性哈希的实例。
	circle *consistent.Consistent

	// nodeManager 是节点管理器，用于管理节点。
	nodeManager *memberlist.Memberlist
}

// newNode 创建一个节点实例，并使用 options 去初始化。
func newNode(options *Options) (*node, error) {
	if options.Cluster == nil || len(options.Cluster) == 0 {
		options.Cluster = []string{options.Address}
	}

	nodeManager, err := createNodeManager(options)
	if err != nil {
		return nil, err
	}

	node := &node{
		options:     options,
		address:     helpers.JoinAddressAndPort(options.Address, options.Port),
		circle:      consistent.New(),
		nodeManager: nodeManager,
	}

	node.circle.NumberOfReplicas = options.VirtualNodeCount
	node.autoUpdateCircle()
	return node, nil
}

func createNodeManager(options *Options) (*memberlist.Memberlist, error) {
	config := memberlist.DefaultLANConfig()
	config.Name = helpers.JoinAddressAndPort(options.Address, options.Port)
	config.BindAddr = options.Address
	config.LogOutput = ioutil.Discard

	nodeManager, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	_, err = nodeManager.Join(options.Cluster)
	return nodeManager, err
}

func (n *node) nodes() []string {
	members := n.nodeManager.Members()
	nodes := make([]string, len(members))
	for i, member := range members {
		nodes[i] = member.Name
	}
	return nodes
}

func (n *node) selectNode(name string) (string, error) {
	return n.circle.Get(name)
}

func (n *node) isCurrentNode(address string) bool {
	return n.address == address
}

func (n *node) updateCircle() {
	n.circle.Set(n.nodes())
}

func (n *node) autoUpdateCircle() {
	n.updateCircle()
	go func() {
		ticker := time.NewTicker(time.Duration(n.options.UpdateCircleDuration) * time.Second)
		for {
			select {
			case <-ticker.C:
				n.updateCircle()
			}
		}
	}()
}
