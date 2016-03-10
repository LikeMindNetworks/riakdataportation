package client

import (
	"errors"
	"net"
	"io"
	"log"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"

	riakprotobuf "github.com/likemindnetworks/riakdataportation/protobuf"
)

type Client struct {
	host string
	conns chan *net.TCPConn
	connCnt int
	connMutex sync.RWMutex
}

var (
	BadResponseLength = errors.New("Response length too short")
	BadNumberOfConnections = errors.New("Connection count <= 0")
	ChanWaitTimeout = errors.New("Waiting for an available connection timed out")
)

func NewClient(host string, connCnt int) *Client {
	chanBufSize := connCnt;

	if (chanBufSize < 1) {
		chanBufSize = 1
	}

	cli := &Client{
		host: host,
		connCnt: connCnt,
		conns: make(chan *net.TCPConn, chanBufSize),
	}

	return cli
}

// Connects to a Riak server.
func (c *Client) Connect() error {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	dialer := new(net.Dialer)
	dialer.Timeout = 10 * time.Second

	tcpaddr, err := net.ResolveTCPAddr("tcp", c.host)
	if err != nil {
		return err
	}

	if c.connCnt <= 0 {
		return BadNumberOfConnections
	}

	// Create multiple connections to Riak
	// and send these to the conns channel for later use
	for i := 0; i < c.connCnt; i++ {
		conn, err := dialer.Dial("tcp", tcpaddr.String())

		if err != nil {
			log.Printf("Connection #%d to %s failed", i, tcpaddr)

			// Empty the conns channel before returning,
			// in case an error appeared after a few
			// successful connections.
			for j := 0; j < i; j++ {
				(<-c.conns).Close()
			}

			c.conns <- nil
			return err
		}

		c.conns <- conn.(*net.TCPConn)
	}

	log.Printf("[%d] connections to %s successful", c.connCnt, tcpaddr)
	return nil
}

// Close the connection
func (c *Client) Close() {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	// Close all the connections
	for i := 0; i < c.connCnt; i++ {
		conn := <-c.conns

		if (conn != nil) {
			conn.Close()
		}
	}

	log.Printf("[%d] connections closed", c.connCnt)
	c.conns <- nil
}

// Releases the TCP connection for use by subsequent requests
func (c *Client) ReleaseConnection(conn *net.TCPConn) {
	// Return this connection down the channel for re-use
	// log.Printf("Release tcp connection")
	c.conns <- conn
}

// send message, returns err, connection, and raw message including header
func (c *Client) SendMessage(
		req proto.Message, code byte,
) (err error, conn *net.TCPConn, msgbuf []byte) {
	conn = <-c.conns
	if conn == nil {
		return BadNumberOfConnections, nil, nil
	}

	// Serialize the request using protobuf
	pbmsg, err := proto.Marshal(req)
	if err != nil {
		return err, nil, nil
	}

	// Build message with header: <length:32> <msg_code:8> <pbmsg>
	i := int32(len(pbmsg) + 1)
	msgbuf = []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), code}
	msgbuf = append(msgbuf, pbmsg...)

	// Send to Riak
	_, err = conn.Write(msgbuf)

	// log.Printf("Msg code: %d Msg size: %d sent", code, i)

	// If an error occurred when sending request
	if err != nil {
		// Make sure connection will be released in the end
		defer c.ReleaseConnection(conn)

		var errno syscall.Errno

		// If the error is not recoverable like a broken pipe
		// , close all connections,
		if operr, ok := err.(*net.OpError); ok {
			if errno, ok = operr.Err.(syscall.Errno); ok {
				if errno == syscall.EPIPE {
					c.Close()
				}
			}
		}
	}

	return
}

// receive message, returns error, header bytes, and body bytes separately
func (c *Client) ReceiveRawMessage(
		conn *net.TCPConn, keepAlive bool,
) (err error, headerbuf []byte, responsebuf []byte) {

	// Read the response from Riak
	err, headerbuf = c.read(conn, 5)

	if err != nil {
		c.ReleaseConnection(conn)

		if err == io.EOF {
			// Connection was closed, try to re-open the connection so subsequent
			// i/o can succeed. Does report the error for this response.
			c.Close()
		}

		return err, nil, nil
	}

	if (!keepAlive) {
		defer c.ReleaseConnection(conn)
	}

	// Check the length
	if len(headerbuf) < 5 {
		return BadResponseLength, nil, nil
	}

	// Read the message length, read the rest of the message if necessary
	msglen := int(headerbuf[0]) << 24 +
		int(headerbuf[1]) << 16 +
		int(headerbuf[2]) << 8 +
		int(headerbuf[3])

	err, responsebuf = c.read(conn, msglen - 1)

	return
}

// receive message, deserializes the data and returns a struct.
func (c *Client) ReceiveMessage(
		conn *net.TCPConn, response proto.Message, keepAlive bool,
) (err error) {

	err, headerbuf, responsebuf := c.ReceiveRawMessage(conn, keepAlive)
	if err != nil {
		return err
	}

	// Deserialize,
	// by default the calling method should provide the expected RbpXXXResp
	msgcode := headerbuf[4]
	switch msgcode {
	case riakprotobuf.CodeRpbErrorResp:
		errResp := &riakprotobuf.RpbErrorResp{}
		err = proto.Unmarshal(responsebuf, errResp)
		if err == nil {
			err = errors.New(string(errResp.GetErrmsg()))
		}
	case riakprotobuf.CodeRpbPingResp,
			riakprotobuf.CodeRpbSetClientIdResp,
			riakprotobuf.CodeRpbSetBucketResp,
			riakprotobuf.CodeRpbDelResp:
		return nil
	default:
		// log.Printf("Msg code: %d Msg size: %d received", msgcode, msglen)
		err = proto.Unmarshal(responsebuf, response)
	}

	return err
}

// Read data from the connection
func (c *Client) read(
		conn *net.TCPConn, size int,
) (err error, response []byte) {
	response = make([]byte, size)
	s := 0

	for i := 0; (size > 0) && (i < size); {
		s, err = conn.Read(response[i:size])
		i += s
		if err != nil {
			return
		}
	}

	return
}
