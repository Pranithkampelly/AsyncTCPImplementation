package async

import (
	"context"
	"errors"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/izhw/gnet/gcore"
	"github.com/izhw/gnet/logger"

	"github.com/rs/xid"
)

type Handler struct {
	*gcore.NetEventHandler
	client *Client
}

func NewAsyncHandler(clt *Client) *Handler {
	return &Handler{
		client: clt,
		logger: logger.GlobalSimpleLogger(),
	}
}

func (h *Handler) OnReadMsg(c gcore.Conn, data []byte) error {
	go h.IncomingMessageHandling(data)
  // if we return error it will close the client 
	return nil
}

func (h *Handler) IncomingMessageHandling(data []byte) {
	var res Response
	var err error
	res.Message = data
	tag := h.client.Client.GetTag()
  // add your own parser here, which will return parsed response and matching ID 
	res.Parsed, res.ID, err = h.client.ClientHandler.GetIdFromISOMessage(ctx, tag, data)
	if err != nil {
		return
	}

	call, _ := h.client.getPendingCall(res.ID)
	h.client.removePendingCall(res.ID)
	if call == nil {
		// No need to further process incoming response, as this is not mapped with any pending request
		// this message can be added as part of metric in future
		return
	}
	call.Res = res
	call.Done <- true
}

func (h *Handler) OnWriteError(c gcore.Conn, data []byte, err error) {
	h.logger.Warn(c.GetTag(), "AsyncClient write error:", err.Error())
}
