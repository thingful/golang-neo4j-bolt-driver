package neo4jbolt

import (
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"time"
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
	OpenPool(connStr string, initialCap, maxCap int) (Pool, error)
}

type DriverOptions struct {
	// DialTimeout is the timeout for establishing new connections
	DialTimeout time.Duration

	// ReadTimeout is the timeout for socket reads.
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for socket writes.
	WriteTimeout time.Duration

	// Addr is the connection string
	Addr string

	// TLSConfig is the tls configuration (nil by default)
	TLSConfig *tls.Config

	// ChunkSize is used to set the max chunk size of the bytes to send to Neo4j
	// at once
	ChunkSize uint16
}

// String is our stringer implementation for DriverOptions
func (d *DriverOptions) String() string {
	return fmt.Sprintf(
		"{dial timeout: %s, read timeout: %s, write timeout: %s, address: %s, chunk size: %v}",
		d.DialTimeout,
		d.ReadTimeout,
		d.WriteTimeout,
		d.Addr,
		d.ChunkSize,
	)
}

func DefaultDriverOptions() *DriverOptions {
	return &DriverOptions{
		DialTimeout:  time.Second * 5,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
		TLSConfig:    nil,
		ChunkSize:    math.MaxUint16,
	}
}

type boltDriver struct {
	recorder *recorder
	options  *DriverOptions
}

// NewDriver creates a new Driver object
func NewDriver() Driver {
	return NewDriverWithOptions(DefaultDriverOptions())
}

func NewDriverWithOptions(options *DriverOptions) Driver {
	return &boltDriver{
		options: options,
	}
}

// Open opens a new Bolt connection to the Neo4J database
func (d *boltDriver) Open(connStr string) (driver.Conn, error) {
	if d.options == nil {
		d.options = DefaultDriverOptions()
	}

	d.options.Addr = connStr

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
func (d *boltDriver) OpenPool(connStr string, initialCap, maxCap int) (Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, ErrInvalidCapacity
	}

	d.options.Addr = connStr

	c := &connPool{
		conns:  make(chan *boltConn, maxCap),
		driver: d,
	}

	for i := 0; i < initialCap; i++ {
		conn, err := newBoltConn(d, c)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("unable to fill the pool: %v", err)
		}
		c.conns <- conn
	}

	return c, nil
}

func init() {
	sql.Register("neo4j-bolt", &boltDriver{})
}
