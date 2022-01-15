package caches

// Status 是一个代表缓存信息的结构体
type Status struct {
	// Count 记录着缓存中的数据个数。
	Count int `json:"count"`

	// KeySize 记录着 key 占用的空间大小。
	KeySize int64 `json:"keySize"`

	// ValueSize 记录着 value 占用的空间大小。
	ValueSize int64 `json:"valueSize"`
}

// NewStatus 返回一个缓存信息对象指针
func NewStatus() *Status {
	return &Status{
		Count:     0,
		KeySize:   0,
		ValueSize: 0,
	}
}

// addEntry 可以将key和value的信息记录起来
func (s *Status) addEntry(key string, value []byte) {
	s.Count++
	s.KeySize += int64(len(key))
	s.ValueSize += int64(len(value))
}

// subEntry可以将key和value的信息从Status中减去
func (s *Status) subEntry(key string, value []byte) {
	s.Count--
	s.KeySize -= int64(len(key))
	s.ValueSize -= int64(len(value))
}

// entrySize 返回键值对占用的空间的大小
func (s *Status) entrySize() int64 {
	return s.ValueSize + s.KeySize
}
