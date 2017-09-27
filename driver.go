package neo4jbolt

import (
	"database/sql"
	"database/sql/driver"
	"math"
	"time"

	"github.com/pkg/errors"
)

const (
	// Version is the current version of this driver
	Version = "1.0"

	// ClientID is the id of this client
	ClientID = "GolangNeo4jBolt/" + Version
)

var (
	magicPreamble     = []byte{0x60, 0x60, 0xb0, 0x17}
	supportedVersions = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	handShake          = append(magicPreamble, supportedVersions...)
	noVersionSupported = []byte{0x00, 0x00, 0x00, 0x00}
)

// Driver is a driver allowing connection to Neo4j
// The driver allows you to open a new connection to Neo4j
//
// Implements sql/driver, but also includes its own more neo-friendly interface.
// Some of the features of this interface implement neo-specific features
// unavailable in the sql/driver compatible interface
//
// Driver objects should be THREAD SAFE, so you can use them
// to open connections in multiple threads.  The connection objects
// themselves, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.
type Driver interface {
	// Open opens a sql.driver compatible connection. Used internally
	// by the go sql interface
	Open(connStr string) (driver.Conn, error)

	// OpenNeo opens a Neo-specific connection. This should be used
	// directly when not using the golang sql interface
	OpenNeo(connStr string) (Conn, error)

	// OpenPool opens a Neo-specific connection pool.
	OpenPool(connStr string) (Pool, error)
}

// boltDriver is our concrete driver implementation. Clients will use this
// instance to get connections to Neo4j
type boltDriver struct {
	recorder *recorder
	//options  *DriverOptions

	// dialTimeout is the timeout for establishing new connections
	dialTimeout time.Duration

	// readTimeout is the timeout for socket reads.
	readTimeout time.Duration

	// writeTimeout is the timeout for socket writes.
	writeTimeout time.Duration

	// addr is the connection string
	addr string

	// chunkSize is used to set the max chunk size of the bytes to send to Neo4j
	// at once
	chunkSize uint16

	// poolSize is used to control the size of the connection pool.
	poolSize int
}

// option is a type alias for a our functional configuration
type option func(*boltDriver)

// ChunkSize is a on option that sets the chunksize we use to write data to
// Neo4j
func ChunkSize(chunkSize uint16) option {
	return func(driver *boltDriver) {
		driver.chunkSize = chunkSize
	}
}

// DialTimeout sets the dial timeout in seconds we should wait to connect to to
// Neo4j
func DialTimeout(timeout time.Duration) option {
	return func(driver *boltDriver) {
		driver.dialTimeout = timeout
	}
}

// ReadTimeout sets the dial timeout in seconds we should wait to read data
// from Neo4j
func ReadTimeout(timeout time.Duration) option {
	return func(driver *boltDriver) {
		driver.readTimeout = timeout
	}
}

// WriteTimeout sets the dial timeout in seconds we should wait to write data
// to Neo4j
func WriteTimeout(timeout time.Duration) option {
	return func(driver *boltDriver) {
		driver.writeTimeout = timeout
	}
}

// PoolSize sets the size of the connection pool if used.
func PoolSize(size int) option {
	return func(driver *boltDriver) {
		driver.poolSize = size
	}
}

// NewDriver creates a new Driver object. We may pass in many configuration
// options via the variadic options parameter. These configuration options will
// be applied to the driver in order.
func NewDriver(options ...option) Driver {
	// default options
	driver := &boltDriver{
		dialTimeout:  time.Second * 5,
		readTimeout:  time.Second * 60,
		writeTimeout: time.Second * 60,
		chunkSize:    math.MaxUint16,
	}

	// apply any configuration options
	for _, opt := range options {
		opt(driver)
	}

	return driver
}

// Open opens a new Bolt connection to the Neo4J database
func (d *boltDriver) Open(connStr string) (driver.Conn, error) {
	d.addr = connStr

	return newBoltConn(d, nil) // Never use pooling when using SQL driver
}

// Open opens a new Bolt connection to the Neo4J database. Implements a
// Neo-friendly alternative to sql/driver.
func (d *boltDriver) OpenNeo(connStr string) (Conn, error) {
	conn, err := d.Open(connStr)
	if err != nil {
		return nil, err
	}

	return conn.(Conn), nil
}

// OpenPool opens an returns a new connection pool for interacting with Neo4j.
// The pool contains the Neo-friendly alternative to sql/driver.
func (d *boltDriver) OpenPool(connStr string) (Pool, error) {
	d.addr = connStr

	c := &connPool{
		conns:  make(chan *boltConn, d.poolSize),
		driver: d,
	}

	for i := 0; i < d.poolSize; i++ {
		conn, err := newBoltConn(d, c)
		if err != nil {
			c.Close()
			return nil, errors.Wrap(err, "Error initializing pool")
		}
		c.conns <- conn
	}

	return c, nil
}

func init() {
	sql.Register("neo4j-bolt", NewDriver())
}
