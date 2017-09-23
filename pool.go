package neo4jbolt

import "sync"

type Pool interface {
	// Get returns a new connection from the pool. Closing the connection puts it
	// back into the pool. Closing when the pool is destroyed or full will be
	// counted as an error
	Get() (Conn, error)

	// Close closes the pool and all its connections. After Close() the pool is
	// no longer usable.
	Close()

	// Len returns the current number of connections in the pool.
	Len() int
}

// Factory is a function to create new boltConn instances
type Factory func() (*boltConn, error)

type connPool struct {
	// storage for our boltConn instances
	mu    sync.Mutex
	conns chan *boltConn

	// boltConn factory function
	factory Factory
}

// Close closes the pool and all its connections.
func (c *connPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}

func (c *connPool) Get() (Conn, error) {
	//conns := c.getConns()
	//if conns == nil {
	//	return nil, ErrClosed
	//}

	//select {
	//case conn := <-conns:
	//	if conn == nil {
	//		return nil, ErrClosed
	//	}

	//	return
	return nil, nil
}

func (c *connPool) getConns() chan *boltConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

func (c *connPool) Len() int { return len(c.getConns()) }

// DriverPool is a driver allowing connection to Neo4j with support for connection pooling
// The driver allows you to open a new connection to Neo4j
//
// Driver objects should be THREAD SAFE, so you can use them
// to open connections in multiple threads.  The connection objects
// themselves, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.
type DriverPool interface {
	// OpenPool opens a Neo-specific connection.
	OpenPool() (Conn, error)
	reclaim(*boltConn) error
}

type boltDriverPool struct {
	connStr  string
	maxConns int
	pool     chan *boltConn
}

// NewDriverPool creates a new Driver object with connection pooling
func NewDriverPool(connStr string, max int) (DriverPool, error) {
	d := &boltDriverPool{
		connStr:  connStr,
		maxConns: max,
		pool:     make(chan *boltConn, max),
	}

	for i := 0; i < max; i++ {
		conn, err := newPooledBoltConn(connStr, d)
		if err != nil {
			return nil, err
		}

		d.pool <- conn
	}

	return d, nil
}

// OpenNeo opens a new Bolt connection to the Neo4J database.
func (d *boltDriverPool) OpenPool() (Conn, error) {
	conn := <-d.pool
	if conn.conn == nil {
		if err := conn.initialize(); err != nil {
			return nil, err
		}
	}
	return conn, nil
}

func (d *boltDriverPool) reclaim(conn *boltConn) error {
	var newConn *boltConn
	var err error
	if conn.connErr != nil || conn.closed {
		newConn, err = newPooledBoltConn(d.connStr, d)
		if err != nil {
			return err
		}
	} else {
		// sneakily swap out connection so a reference to
		// it isn't held on to
		newConn = &boltConn{}
		*newConn = *conn
	}

	d.pool <- newConn
	conn = nil

	return nil
}
