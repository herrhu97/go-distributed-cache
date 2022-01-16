package main

import (
	"cache-server/servers"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
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

// go test -v -count=1 performance_test.go -run=^TestHttpServer$
func TestHttpServer(t *testing.T) {

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		request, err := http.NewRequest("PUT", "http://localhost:5837/v1/cache/"+data, strings.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	})

	t.Logf("写入消耗时间为 %s。", writeTime)

	time.Sleep(3 * time.Second)

	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		request, err := http.NewRequest("GET", "http://localhost:5837/v1/cache/"+data, nil)
		if err != nil {
			t.Fatal(err)
		}

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	})

	t.Logf("读取消耗时间为 %s。", readTime)
}

// go test -v -count=1 performance_test.go -run=^TestTcpServer$
func TestTcpServer(t *testing.T) {
	client, err := servers.NewTCPClient("127.0.0.1:5837")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		err := client.Set(data, []byte(data), 0)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Logf("写入的消耗时间为%s", writeTime)

	time.Sleep(3 * time.Second)

	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		_, err := client.Get(data)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Logf("读取的消耗时间为%s", readTime)
}

// go test -v -count=1 redis_test.go -run=^TestRedis$
func TestRedis(t *testing.T) {

	conn, err := redis.DialURL("redis://127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		conn.Do("set", data, data)
	})

	t.Logf("写入消耗时间为 %s。", writeTime)

	time.Sleep(3 * time.Second)

	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		conn.Do("get", data)
	})

	t.Logf("读取消耗时间为 %s。", readTime)
}
