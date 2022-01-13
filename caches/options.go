package caches

// Options 是一些选项的结构体
type Options struct {
	// MaxEntrySize 是写满保护的一个阈值，当缓存中的键值对占用空间达到这个值，就会触发写满保护。
	// 这个值的单位是 GB。
	MaxEntrySize int64

	// MaxGcCount 是自动淘汰机制的一个阈值，当清理的数据达到了这个值后就会停止清理了。
	MaxGcCount int

	// GcDuration 是自动淘汰机制的时间间隔，每隔固定的 GcDuration 时间就会进行一次自动淘汰。
	// 这个值的单位是分钟。
	GcDuration int64

	// DumpFile 是持久化文件的路径。
	DumpFile string

	// DumpDuration 是持久化的时间间隔。
	// 我们知道持久化是需要定时执行的，这个定时的间隔可以是很大，也可以是很小。
	// 很大会导致持久化的数据不够新，假设设置为 1 个星期持久化一次，那么一旦在下一次持久化执行之前缓存崩了，那就丢失一个星期的数据了。
	// 很小会导致持久化太频繁占用性能，假设设置为 1 秒持久化一次，那这个缓存的就几乎一直在进行持久化了。
	// 所以这个值的设定是需要考量的，最起码需要根据业务来定，这里就需要给用户去配置。这个值的单位是分钟。
	DumpDuration int64
}

// DefaultOptions 返回一个默认的选项设置对象
func DefaultOptions() Options {
	return Options{
		MaxEntrySize: int64(4),
		MaxGcCount:   1000,
		GcDuration:   60,
		DumpFile:     "cache-server.dump",
		DumpDuration: 30,
	}
}
