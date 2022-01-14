package caches

import (
	"sync"
	"sync/atomic"
	"time"
)

// Cache是一个结构体，用于封装缓存底层结构的
type Cache struct {
	// segmentSize 是segment的数量
	segmentSize int

	// segments 存储着所有的segment实例
	segments []*segment

	// options 缓存配置。
	options *Options

	// dumping 标识当前缓存是否除于持久化状态，1表示除于持久化状态
	// 因为现在的 cache 是没有全局锁的，而持久化需要记录下当前的状态，不允许有更新，所以使用一个变量记录着，
	// 如果处于持久化状态，就让所有更新操作进入自旋状态，等待持久化完成再进行。
	dumping int32
}

// NewCache 返回一个缓存对象
func NewCache() *Cache {
	return NewCacheWith(DefaultOptions())
}

func NewCacheWith(options Options) *Cache {
	if cache, ok := recoverFromDumpFile(options.DumpFile); ok {
		return cache
	}
	return &Cache{
		segmentSize: options.SegmentSize,

		segments: newSegments(&options),
		options:  &options,
		dumping:  0,
	}
}

// recoverFromDumpFile 从dumpFile中回复缓存
// 如果恢复不成功，就返回nil和false
func recoverFromDumpFile(dumpFile string) (*Cache, bool) {
	cache, err := newEmptyDump().from(dumpFile)
	if err != nil {
		return nil, false
	}
	return cache, true
}

// newSegments 返回初始化好的segment实例列表
func newSegments(options *Options) []*segment {
	// 根据配置的数量生成segment
	segments := make([]*segment, options.SegmentSize)
	for i := 0; i < options.SegmentSize; i++ {
		segments[i] = newSegment(options)
	}
	return segments
}

// index 是选择 segment 的“特殊算法”。
// 这里参考了 Java 中的哈希生成逻辑，尽可能避免重复。不用去纠结为什么这么写，因为没有唯一的写法。
// 为了能使用到哈希值的全部数据，这里使用高位和低位进行异或操作。
func index(key string) int {
	index := 0
	keyBytes := []byte(key)
	for _, b := range keyBytes {
		index = 31*index + int(b&0xff)
	}
	return index ^ (index >> 16)
}

// segmentOf 返回key对应的segment
// 使用index生成的哈希值去获取segment，这里使用&运算也是Java中的技巧
func (c *Cache) segmentOf(key string) *segment {
	return c.segments[index(key)&(c.segmentSize-1)]
}

// Get 返回指定key的value，如果找不到就返回false
func (c *Cache) Get(key string) ([]byte, bool) {
	// 等待持久化完成
	c.waitForDumping()
	return c.segmentOf(key).get(key)
}

// Set 添加一个键值对到缓存中，不设定 ttl，也就意味着数据不会过期。
// 返回 error 是 nil 说明添加成功，否则就是添加失败，可能是触发了写满保护机制，拒绝写入数据。
func (c *Cache) Set(key string, value []byte) error {
	return c.SetWithTTL(key, value, NeverDie)
}

// SetWithTTL 添加一个键值对到缓存中，使用给定的 ttl 去设定过期时间。
func (c *Cache) SetWithTTL(key string, value []byte, ttl int64) error {
	c.waitForDumping()
	return c.segmentOf(key).set(key, value, ttl)
}

// Delete删除指定key的键值对数据
func (c *Cache) Delete(key string) {
	c.waitForDumping()
	c.segmentOf(key).delete(key)
}

// Status 返回缓存信息。
func (c *Cache) Status() Status {
	result := newStatus()
	for _, segment := range c.segments {
		status := segment.status()
		result.Count += status.Count
		result.KeySize += status.KeySize
		result.ValueSize += status.ValueSize
	}
	return *result
}

// gc 会触发数据清理任务，主要是清理过期的数据。
func (c *Cache) gc() {
	c.waitForDumping()
	wg := &sync.WaitGroup{}
	for _, seg := range c.segments {
		wg.Add(1)
		go func(s *segment) {
			defer wg.Done()
			s.gc()
		}(seg)
	}
	wg.Wait()
}

// AutoGc 会开启一个定时 GC 的异步任务。
func (c *Cache) AutoGc() {
	go func() {
		// 根据配置中的 GcDuration 来设置定时的间隔
		ticker := time.NewTicker(time.Duration(c.options.GcDuration) * time.Minute)
		for {
			// 使用 select 来判断是否达到了定时器的触发点
			// 当定时器的时间还没到的时候，ticker.C 管道会被阻塞
			// 当定时器的时间到达后，就会向 ticker.C 管道中发送当前时间，停止阻塞，执行 c.gc() 代码
			select {
			case <-ticker.C:
				c.gc()
			}
		}
	}()
}

// dump 持久化缓存方法
func (c *Cache) dump() error {
	// 这边使用 atomic 包中的原子操作完成状态的切换
	atomic.StoreInt32(&c.dumping, 1)
	defer atomic.StoreInt32(&c.dumping, 0)
	return newDump(c).to(c.options.DumpFile)
}

// AutoDump 开启定时任务去持久化缓存。
// 和自动 Gc 的原理是一样的，这里就不再赘述了。
func (c *Cache) AutoDump() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.options.DumpDuration) * time.Minute)
		for {
			select {
			case <-ticker.C:
				c.dump()
			}
		}
	}()
}

// waitForDumping 会等待持久化完成才返回
func (c *Cache) waitForDumping() {
	for atomic.LoadInt32(&c.dumping) != 0 {
		// 每次循环都会等待一定的时间，如果不睡眠，会导致 CPU 空转消耗资源
		time.Sleep(time.Duration(c.options.CasSleepTime) * time.Microsecond)
	}
}
