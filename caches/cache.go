package caches

import (
	"errors"
	"sync"
	"time"
)

// Cache是一个结构体，用于封装缓存底层结构的
type Cache struct {
	// data 存储着实际的键值对数据。
	data map[string]*value

	// options 存储着选项设置。
	// 这里不使用指针类型，因为这是一个配置，当外界传进来之后，里面的值就固定了。
	// 使用值传递也是为了更明确地表达这一点：options 是不会在内部修改的，属于只读状态的。
	// 这是我的代码风格，也算是我在代码可读性上的一点小强迫症吧，没有说一定要这么做哈。
	options Options

	// status 表示缓存的状态信息。
	// 这里使用的是指针类型，因为这个 status 会在内部不断地更新，所以想明确地表达出这个值是会被修改的。
	status *Status

	// lock 保证并发安全的锁
	lock *sync.RWMutex
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
		// 这里指定 256 的初始容量是为了减少哈希冲突的几率和扩容带来的性能损失
		data:    make(map[string]*value, 256),
		options: options,
		status:  newStatus(),
		lock:    &sync.RWMutex{},
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

// Get 返回指定key的value，如果找不到就返回false
func (c *Cache) Get(key string) ([]byte, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	value, ok := c.data[key]
	if !ok {
		return nil, false
	}

	// 如果数据不是存活的，将数据删除掉，返回找不到数据
	// 注意这边对锁的操作，由于一开始加的是读锁，无法保证写的并发安全，而删除需要加写锁，读锁和写锁又是互斥的
	// 所以先将读锁释放，再上写锁，删除操作里面会加写锁，删除完之后，写锁释放，我们再上读锁
	if !value.alive() {
		c.lock.RUnlock()
		c.Delete(key)
		c.lock.RLock()
		return nil, false
	}

	// 注意这个 visit 方法会使用 Swap 的形式更新数据的创建时间，用于实现 LRU 过期机制
	return value.visit(), true
}

// Set 添加一个键值对到缓存中，不设定 ttl，也就意味着数据不会过期。
// 返回 error 是 nil 说明添加成功，否则就是添加失败，可能是触发了写满保护机制，拒绝写入数据。
func (c *Cache) Set(key string, value []byte) error {
	return c.SetWithTTL(key, value, NeverDie)
}

// SetWithTTL 添加一个键值对到缓存中，使用给定的 ttl 去设定过期时间。
func (c *Cache) SetWithTTL(key string, value []byte, ttl int64) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if oldValue, ok := c.data[key]; ok {
		// 如果是已经存在的 key，就不属于新增键值对了，为了方便处理，先把原本的键值对信息去除
		c.status.subEntry(key, oldValue.Data)
	}

	// 这边会判断缓存的容量是否足够，如果不够了，就返回写满保护的错误信息
	if !c.checkEntrySize(key, value) {
		// 注意刚刚把旧的键值对信息去除了，现在要加回去，因为并没有添加新的键值对
		if oldValue, ok := c.data[key]; ok {
			c.status.addEntry(key, oldValue.Data)
		}

		// 使用 errors 包的方法创建一个简单的错误
		// 一般会把这个错误定义到全局的变量中，方便使用者判断，而且也可以减少相同错误的重复创建
		// 这里为了方便，就先使用 New 方法创建了，有兴趣的童鞋可以尝试抽取为全局错误
		return errors.New("the entry size will exceed if you set this entry")
	}

	// 添加新的键值对，需要先更新缓存信息，然后保存数据
	c.status.addEntry(key, value)
	c.data[key] = newValue(value, ttl)
	return nil

}

// Delete删除指定key的键值对数据
func (c *Cache) Delete(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if oldValue, ok := c.data[key]; ok {
		// 如果存在这个 key 才会进行删除，并且需要先把缓存信息更新掉
		c.status.subEntry(key, oldValue.Data)
		delete(c.data, key)
	}
}

// Status 返回缓存信息。
func (c *Cache) Status() Status {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// 注意到这个返回值是 Status 而不是 *Status，这意味着返回的是一个副本，而不是本来的对象
	// 这么做的原因是防止外界直接修改缓存信息
	return *c.status
}

// checkEntrySize 会检查要添加的键值对是否满足当前缓存的要求。
func (c *Cache) checkEntrySize(newKey string, newValue []byte) bool {
	// 将当前的键值对占用空间加上要被添加的键值对占用空间，然后和配置中的最大键值对占用空间进行比较
	return c.status.entrySize()+int64(len(newKey))+int64(len(newValue)) <= c.options.MaxEntrySize*1024*1024
}

// gc 会触发数据清理任务，主要是清理过期的数据。
func (c *Cache) gc() {
	c.lock.Lock()
	defer c.lock.Unlock()
	// 使用 count 记录当前清理的个数
	count := 0
	for key, value := range c.data {
		if !value.alive() {
			c.status.subEntry(key, value.Data)
			delete(c.data, key)

			// 清理之后更新个数，并且判断是否已经达到配置中的最大清理个数
			// 这里记录的是清理的个数，也就意味着可能会遍历完整个 map 都不一定会触发 break
			// 其实还可以改为记录遍历的个数，这样就可以避免数据太多的时候服务停在 GC 上的情况了
			count++
			if count >= c.options.MaxGcCount {
				break
			}
		}
	}
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
func (c *Cache) dump() error{
	c.lock.Lock()
	defer c.lock.Unlock()
	return newDump(c).to(c.options.DumpFile)
}

// AutoDump 开启定时任务去持久化缓存。
// 和自动 Gc 的原理是一样的，这里就不再赘述了。
func (c *Cache) AutoDump() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.options.DumpDuration) * time.Minute)
		for {
			select {
				case <- ticker.C:
					c.dump()
			}
		}
	}()
}