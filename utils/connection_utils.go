package utils

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

const (
	connected = "200 Connected to Go RPC"
)

// DialHTTP is a wrapper around rpc.DialHTTP. It is used to connect to an HTTP
// RPC serverat the specified network address. Differently from rpc.DialHTTP,
// a connection with a timeout is used
func DialHTTP(network, address string, d time.Duration) (*rpc.Client, error) {
	var err error
	conn, err := net.DialTimeout(network, address, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
	conn.SetDeadline(time.Now().Add(d))

	// Require successful HTTP response
	// before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn),
		&http.Request{Method: "CONNECT"})

	if err == nil && resp.Status == connected {
		return rpc.NewClient(conn), nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, &net.OpError{
		Op:   "dial-http",
		Net:  network + " " + address,
		Addr: nil,
		Err:  err,
	}
}
