package client

import (
	"encoding/binary"

	"github.com/FishGoddess/vex"
)

const (
	getCommand = byte(1)

	setCommand = byte(2)

	deleteCommand = byte(3)

	statusCommand = byte(4)
)

type AsyncClient struct {
	client *vex.Client

	requestChan chan *request
}

func NewAsyncClient(address string) (*AsyncClient, error) {
	client, err := vex.NewClient("tcp", address)
	if err != nil {
		return nil, err
	}

	c := &AsyncClient{
		client:      client,
		requestChan: make(chan *request, 163840),
	}
	c.handleRequest()
	return c, nil
}

func (ac *AsyncClient) handleRequest() {
	go func() {
		for request := range ac.requestChan {
			body, err := ac.client.Do(request.command, request.args)
			request.resultChan <- &Response{
				Body: body,
				Err:  err,
			}
		}
	}()
}

func (ac *AsyncClient) do(command byte, args [][]byte) <-chan *Response {
	resultChan := make(chan *Response, 1)
	ac.requestChan <- &request{
		command:    command,
		args:       args,
		resultChan: resultChan,
	}
	return resultChan
}

func (ac *AsyncClient) Get(key string) <-chan *Response {
	return ac.do(getCommand, [][]byte{[]byte(key)})
}

func (ac *AsyncClient) Set(key string, value []byte, ttl int64) <-chan *Response {
	ttlBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBytes, uint64(ttl))
	return ac.do(setCommand, [][]byte{
		ttlBytes, []byte(key), value,
	})
}

func (ac *AsyncClient) Delete(key string) <-chan *Response {
	return ac.do(deleteCommand, [][]byte{[]byte(key)})
}

func (ac *AsyncClient) Status() <-chan *Response {
	return ac.do(statusCommand, nil)
}

func (ac *AsyncClient) Close() error {
	close(ac.requestChan)
	return ac.client.Close()
}
