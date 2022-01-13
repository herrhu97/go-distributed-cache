package servers

import (
	"cache-server/caches"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// HTTPServer 是http服务器结构
type HTTPServer struct {
	cache *caches.Cache
}

// NewHTTPServer 返回一个关于cache的新HTTP服务器
func NewHTTPServer(cache *caches.Cache) *HTTPServer {
	return &HTTPServer{
		cache: cache,
	}
}

// Run 启动服务器
func (hs *HTTPServer) Run(address string) error {
	return http.ListenAndServe(address, hs.routerHandler())
}

// routerHandler 返回路由处理器给http包中注册用
func (hs *HTTPServer) routerHandler() http.Handler {
	router := httprouter.New()
	router.GET("/cache/:key", hs.getHandler)
	router.PUT("/cache/:key", hs.setHandler)
	router.DELETE("/cache/:key", hs.deleteHandler)
	router.GET("/status", hs.statusHandler)
	return router
}

// getHandler 用于获取缓存数据
func (hs *HTTPServer) getHandler(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	key := params.ByName("key")
	value, ok := hs.cache.Get(key)
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	writer.Write(value)
}

// setHandler 用于保存缓存数据
func (hs *HTTPServer) setHandler(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	key := params.ByName("key")
	value, err := ioutil.ReadAll(request.Body)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	hs.cache.Set(key, value)
}

// deleteHandler 用于删除缓存数据
func (hs *HTTPServer) deleteHandler(writer http.ResponseWriter, r *http.Request, params httprouter.Params) {
	key := params.ByName("key")
	hs.cache.Delete(key)
}

// statusHandler 用于获取缓存键值对的个数
func (hs *HTTPServer) statusHandler(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	status, err := json.Marshal(map[string]interface{}{
		"count": hs.cache.Count(),
	})

	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Write(status)
}
