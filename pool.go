package neo4jbolt

import (
	"errors"
	"sync"
	"time"

	"github.com/thingful/golang-neo4j-bolt-driver/log"
)

type Pool interface {
	// Get returns a new connection from the pool. Closing the connection puts it
	// back into the pool. Closing when the pool is destroyed or full will be
	// counted as an error
	Get() (Conn, error)

	// Close closes the pool and all its connections. After Close() the pool is
	// no longer usable.
	Close()
}

type connPool struct {
	mu    sync.Mutex
	conns chan *boltConn

	driver *boltDriver
}

// Close closes the pool and all its connections.
func (c *connPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.driver = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.conn.Close()
	}
}

func (c *connPool) Get() (Conn, error) {
	log.Info("Getting conn from pool")
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	var conn *boltConn
	var err error

	select {
	case conn = <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
	case <-time.After(c.driver.poolTimeout):
		return nil, ErrPoolTimeout
	default:
		conn, err = newBoltConn(c.driver, c)
		if err != nil {
			return nil, err
		}
	}

	return conn, nil
}

// put puts the connection back into the pool. If the pool is full or closed
// the underlying net.Conn is simply closed. A nil conn will be rejected.
func (c *connPool) put(conn *boltConn) error {
	if conn == nil {
		return errors.New("connection is nil, rejecting")
	}

	log.Info("Putting conn back into pool")

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		// pool is closed, close passed connection - note we close the wrapped
		// net.Conn not our wrapper
		return conn.conn.Close()
	}

	select {
	case c.conns <- conn:
		return nil
	default:
		// pool is full, close passed connection - note we close the wrapped
		// net.Conn, not our wrapper
		return conn.conn.Close()
	}
}

func (c *connPool) getConns() chan *boltConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}
