package servers

// Server 是服务器结构的接口
type Server interface {
	Run(address string) error
}