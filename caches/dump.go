package caches

import (
	"encoding/gob"
	"os"
	"sync"
	"time"
)

// dump 是我们需要进行持久化的一个结构。
// 其实直接持久化 Cache 结构体也可以，但是 Gob 必须要有导出字段才可以进行序列化，
// 而我们的 Cache 是没有导出字段的，也不需要导出任何字段，所以直接持久化 Cache 的改造不太适合。
// 更适合的设计是新增一个专门用于持久化的结构，然后进行结构的转换，就像这个 dump 结构到 Cache 结构一样。
// 于是，dump 结构的成员就和 Cache 的差不多，只不过都是导出字段用于 Gob 序列化。
// 如果某个结构体中没有导出字段，那么在 Gob 序列化的时候就会出错。
// 比如我们在 dump 中引用了 value 结构，那么 value 结构中就必须至少有一个导出字段，否则序列化就会出错。
type dump struct {
	// Data 存储具体的键值对。
	Data map[string]*value

	// Options 记录着缓存的选项配置。
	Options Options

	// Status 记录着缓存的信息。
	Status *Status
}

// newEmptyDump 创建一个空的dump结构对象并返回
func newEmptyDump() *dump {
	return &dump{}
}

// newDump 创建一个dump对象并使用指定的Cache对象初始化
func newDump(c *Cache) *dump {
	return &dump{
		Data:    c.data,
		Options: c.options,
		Status:  c.status,
	}
}

// nowSuffix 返回一个类似于20060102150405的文件后缀名
func nowSuffix() string {
	return "." + time.Now().Format("20060102150405")
}

// to 会将 dump 持久化到 dumpFile 中。
// 这个 dumpFile 是指定的一个文件。
func (d *dump) to(dumpFile string) error {
	// 使用 os.OpenFile 打开一个文件，os.O_CREATE 表示如果文件不存在就新建
	// 由于持久化文件是需要写入，而且每次写入时必须是空文件，否则会和上次的持久化数据混淆，所以需要指定 os.O_TRUNC
	// 这样一旦在持久化的过程中出现问题，没有持久化成功，而原本的持久化文件已经被清空了，就会导致之前的持久化数据全部毁于一旦
	// 这是很可怕的一件事情，所以需要生成新的持久化文件，并且持久化到新的文件中，持久化成功之后再删除原本的持久化文件
	// 这样就可以保证持久化数据至少有个备份，更安全
	newDumpFile := dumpFile + nowSuffix()
	file, err := os.OpenFile(newDumpFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	err = gob.NewEncoder(file).Encode(d)
	if err != nil {
		// 注意这里需要先把文件关闭了，不然 os.Remove 是没有权限删除这个文件的
		file.Close()
		os.Remove(newDumpFile)
		return err
	}

	// 将旧的持久化文件删除
	os.Remove(dumpFile)

	// 将新的持久化文件改名为旧的持久化名字，相当于替换，这样可以保证持久化文件的名字不变
	// 注意这里需要先把文件关闭了，不然 os.Rename 是没有权限重命名这个文件的
	file.Close()
	return os.Rename(newDumpFile, dumpFile)
}

// from 会从 dumpFile 中恢复数据到一个 Cache 结构对象并返回。
func (d *dump) from(dumpFile string) (*Cache, error) {
	// 读取 dumpFile 文件并使用反序列化器进行反序列化
	file, err := os.Open(dumpFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if err = gob.NewDecoder(file).Decode(d); err != nil {
		return nil, err
	}

	// 然后初始化一个缓存对象并返回
	return &Cache{
		data:    d.Data,
		options: d.Options,
		status:  d.Status,
		lock:    &sync.RWMutex{},
	}, nil
}
