package caches

import (
	"cache-server/helpers"
	"sync"
)

// Cache是一个结构体，用于封装缓存底层结构的
type Cache struct {
	data map[string][]byte

	count int64

	lock *sync.RWMutex
}

// NewCache 返回一个缓存对象
func NewCache() *Cache {
	return &Cache{
		data:  make(map[string][]byte, 256),
		count: 0,
		lock:  &sync.RWMutex{},
	}
}

// Get 返回指定key的value，如果找不到就返回false
func (c *Cache) Get(key string) ([]byte, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	value, ok := c.data[key]
	return value, ok
}

// Set 保存key和value到缓存中
func (c *Cache) Set(key string, value []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.data[key]
	if !ok {
		c.count++
	}
	c.data[key] = helpers.Copy(value)
}

// Delete删除指定key的键值对数据
func (c *Cache) Delete(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.data[key]
	if ok {
		c.count--
		delete(c.data, key)
	}
}

// Count 返回键值对数据的个数
func (c *Cache) Count() int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.count
}
