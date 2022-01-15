package client

import (
	"strconv"
	"testing"
	"time"
)

const (
	keySize = 10000
)

func testTask(task func(no int)) string {
	beginTime := time.Now()
	for i := 0; i < keySize; i++ {
		task(i)
	}
	return time.Now().Sub(beginTime).String()
}

func TestAsyncClientPerformance(t *testing.T) {
	client, err := NewAsyncClient(":5837")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		// 等待结果的返回
		<-client.Set(data, []byte(data), 0)
	})

	t.Logf("写入消耗时间%s", writeTime)

	time.Sleep(3 * time.Second)

	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		// 等待结果的返回
		<-client.Get(data)
	})

	t.Logf("读取消耗时间为%s", readTime)

	time.Sleep(time.Second)
}
