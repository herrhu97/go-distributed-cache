package main

import (
	"cache-server/caches"
	"cache-server/servers"
	"flag"
	"log"
)

func main() {
	// 解析所有的 flag
	// 因为我们加入了一些用户配置，options 还记得吗？所以我们需要给用户一个入口去修改这些配置
	// 这是修改监听地址的参数，默认是 5837 端口
	address := flag.String("address", ":5837", "The address used to listen, such as 127.0.0.1:5837.")

	// 创建一个默认配置
	// 下面几个 flag 就是修改对应的配置参数用的
	options := caches.DefaultOptions()
	flag.IntVar(&options.MaxEntrySize, "maxEntrySize", options.MaxEntrySize, "The max memory size that entries can use. The unit is GB.")
	flag.IntVar(&options.MaxGcCount, "maxGcCount", options.MaxGcCount, "The max count of entries that gc will clean.")
	flag.IntVar(&options.GcDuration, "gcDuration", options.GcDuration, "The duration between two gc tasks. The unit is Minute.")
	flag.StringVar(&options.DumpFile, "dumpFile", options.DumpFile, "The file used to dump the cache.")
	flag.IntVar(&options.DumpDuration, "dumpDuration", options.DumpDuration, "The duration between two dump tasks. The unit is Minute.")
	flag.IntVar(&options.MapSizeOfSegment, "mapSizeOfSegment", options.MapSizeOfSegment, "The map size of segment.")
	flag.IntVar(&options.SegmentSize, "segmentSize", options.SegmentSize, "The number of segment in a cache. This value should be the pow of 2 for precision.")
	flag.IntVar(&options.CasSleepTime, "casSleepTime", options.CasSleepTime, "The time of sleep in one cas step. The unit is Microsecond.")

	// 添加一个 flag 用于选择启动的服务端类型，默认是 tcp
	serverType := flag.String("serverType", "tcp", "The type of server (http, tcp).")
	flag.Parse()

	cache := caches.NewCacheWith(options)
	cache.AutoGc()

	// 开启自动进行持久化任务
	cache.AutoDump()

	// 记录日志，能知道缓存服务是否启动了
	log.Printf("Kafo is running on %s at %s.", *serverType, *address)

	err := servers.NewServer(*serverType, cache).Run(*address)
	if err != nil {
		panic(err)
	}
}
