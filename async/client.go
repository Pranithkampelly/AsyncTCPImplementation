package async

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	"github.com/izhw/gnet"
	"github.com/izhw/gnet/gcore"
	"github.com/izhw/gnet/logger"
)

const Async = "async"

// Request represents a request from client
type Request struct {
	ID string `json:"id"`
}

// Response is the reply message from the server
type Response struct {
	Parsed  map[string]string `json:"parsed"`
	ID      string            `json:"id"`
	Message []byte            `json:"message"`
}

// Call represents an active request
type Call struct {
	Req       Request
	Res       Response
	Done      chan bool
	Error     error
}

type Client struct {
	mu            sync.RWMutex
	Client        gnet.Client
	ClientHandler options.RequestResponseHandler
	Pending       map[string]*Call
}

func (c *Client) getPendingCall(id string) (*Call, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	call, ok := c.Pending[id]
	return call, ok
}

func (c *Client) setPendingCall(id string, call *Call) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Pending[id] = call
}

func (c *Client) removePendingCall(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.Pending, id)
}

func NewCall(req Request) *Call {
	done := make(chan bool)
	return &Call{
		Req:  req,
		Done: done,
	}
}

func GetAsyncClient(settings options.TcpSettings) (*Client, error) {
	ws := &Client{
		Pending: make(map[string]*Call, 1),
	}

	log := logger.GlobalSimpleLogger()
	svc := gnet.NewService(
		gcore.WithServiceType(gcore.SvcTypeTCPAsyncClient),
		gcore.WithAddr(settings.BaseUrl),
		gcore.WithEventHandler(NewAsyncHandler(ws)),
		gcore.WithLogger(log),
		gcore.WithReadTimeout(settings.TcpOptions.ReadTimeout),
		gcore.WithWriteTimeout(settings.TcpOptions.WriteTimeout),
		gcore.WithBufferLen(settings.TcpOptions.InitReadBufLen, settings.TcpOptions.MaxReadBufLen),
		gcore.WithHeaderCodec(settings.TcpOptions.HeaderCodec),
	)

	ctx := context.WithValue(context.TODO(), connNetworkKey, settings.TcpOptions.Tag)
	c := svc.Client()
	err := c.Init()
	if err != nil {
		return nil, err
	}
	
	time.Sleep(time.Duration(10) * time.Millisecond)
	if c.Closed() {
		return nil, errors.New("connection is closed")
	}
	c.SetTag(settings.TcpOptions.Tag)
	ws.Client = c
	ws.ClientHandler = settings.Handler
	return ws, nil
}

func (c *Client) SendRequest(ctx context.Context, id string, payload []byte) (map[string]string, []byte, error) {
	req := Request{ID: id}
	call := NewCall(req)
	c.setPendingCall(id, call)
	defer c.removePendingCall(id)
  
	err := c.Client.Write(payload)
	if err != nil {
		return nil, nil, err
	}

	select {
	case <-call.Done:
	case <-time.After(GetRequestTimeout(c.Client.GetTag())):
		call.Error = errors.New(TimeOutError)
	}
	if call.Error != nil {
		return nil, nil, call.Error
	}
  
	return call.Res.Parsed, call.Res.Message, nil
}

func (c *Client) IsClosed() bool {
	closed := c.Client.Closed()
	return closed
}

func GetRequestTimeout(tag string) time.Duration {
	return 30 * time.Second
}

func (p *Client) Destroy() error {
	return p.Client.Close()
}
