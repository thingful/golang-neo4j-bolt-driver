package neo4jbolt

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
