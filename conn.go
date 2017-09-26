package neo4jbolt

import (
	"bytes"
	"crypto/tls"
	"database/sql/driver"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/thingful/golang-neo4j-bolt-driver/encoding"
	"github.com/thingful/golang-neo4j-bolt-driver/errors"
	"github.com/thingful/golang-neo4j-bolt-driver/log"
	"github.com/thingful/golang-neo4j-bolt-driver/structures/messages"
)

// Conn represents a connection to Neo4J
//
// Implements a neo-friendly interface.  Some of the features of this interface
// implement neo-specific features unavailable in the sql/driver compatible
// interface
//
// Conn objects, and any prepared statements/transactions within ARE NOT THREAD
// SAFE.  If you want to use multipe go routines with these objects, you should
// use a driver to create a new conn for each routine.
type Conn interface {
	// PrepareNeo prepares a neo4j specific statement
	PrepareNeo(query string) (Stmt, error)

	// PreparePipeline prepares a neo4j specific pipeline statement Useful for
	// running multiple queries at the same time
	PreparePipeline(query ...string) (PipelineStmt, error)

	// QueryNeo queries using the neo4j-specific interface
	QueryNeo(query string, params map[string]interface{}) (Rows, error)

	// QueryNeoAll queries using the neo4j-specific interface and returns all row
	// data and output metadata
	QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error)

	// QueryPipeline queries using the neo4j-specific interface
	// pipelining multiple statements
	QueryPipeline(query []string, params ...map[string]interface{}) (PipelineRows, error)

	// ExecNeo executes a query using the neo4j-specific interface
	ExecNeo(query string, params map[string]interface{}) (Result, error)

	// ExecPipeline executes a query using the neo4j-specific interface
	// pipelining multiple statements
	ExecPipeline(query []string, params ...map[string]interface{}) ([]Result, error)

	// Close closes the connection
	Close() error

	// Begin starts a new transaction
	Begin() (driver.Tx, error)
}

// newBoltConn is our Factory implementation for creating a new bolt connection
func newBoltConn(driver *boltDriver, pool *connPool) (*boltConn, error) {
	u, err := parseURL(driver.options.Addr)
	if err != nil {
		return nil, err
	}

	username, password, err := parseAuth(u)
	if err != nil {
		return nil, err
	}

	c := &boltConn{
		options:       driver.options,
		url:           u,
		user:          username,
		password:      password,
		serverVersion: make([]byte, 4),

		pool: pool,
	}

	log.Trace("Bolt Host: ", u.Host)
	log.Trace("User: ", username)
	log.Trace("Password: ", password)
	log.Trace("TLS Config: ", c.options.TLSConfig)

	// initialize the connection
	err = c.initialize(driver)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred initializing connection")
	}

	return c, nil
}

// parseURL parses the address string, and checks for the correct connection
// scheme
func parseURL(addr string) (*url.URL, error) {
	log.Infof("Parsing URL: %s\n", addr)

	// parse the connection url here, and then initialize the boltConn instance
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred while parsing url")
	}

	if strings.ToLower(u.Scheme) != "bolt" {
		return nil, errors.New("Unsupported connection string scheme: %s. Driver only supports 'bolt'", u.Scheme)
	}

	return u, nil
}

func parseAuth(u *url.URL) (string, string, error) {
	log.Info("Parsing auth credentials")

	if u.User != nil {
		username := u.User.Username()
		password, isSet := u.User.Password()
		if isSet {
			return username, password, nil
		}

		return "", "", errors.New("Must specify password when passing user")
	}

	return "", "", nil
}

//func parseTimeout(u *url.URL) (timeout time.Duration, err error) {
//	t := u.Query().Get("timeout")
//	if t != "" {
//		val, err := strconv.Atoi(t)
//		if err != nil {
//			return timeout, errors.New("Invalid format for timeout: %s. Must be integer", timeout)
//		}
//
//		timeout = time.Duration(val) * time.Second
//	}
//
//	return timeout, err
//}

//func parseTLSConfig(u *url.URL) (*tls.Config, error) {
//	useTLS = strings.HasPrefix(strings.ToLower(u.Query().Get("tls")), "t") || u.Query().Get("tls") == "1"
//
//	if !useTLS {
//		return nil, nil
//	}
//
//	certFile := url.Query().Get("tls_cert_file")
//	keyFile := url.Query().Get("tls_key_file")
//	caCertFile := url.Query().Get("tls_ca_cert_file")
//	tlsNoVerify := strings.HasPrefix(strings.ToLower(url.Query().Get("tls_no_verify")), "t") ||
//		url.Query().Get("tls_no_verify") == "1"
//
//	var (
//		caCertPool *x509.CertPool
//		cert       tls.Certificate
//	)
//
//	if caCertFile != "" {
//		caCert, err := ioutil.ReadFile(caCertFile)
//		if err != nil {
//			return nil, err
//		}
//
//		caCertPool = x509.NewCertPool()
//		caCertPool.AppendCertsFromPEM(caCert)
//	}
//
//	if certFile != "" {
//		if keyFile == "" {
//			return nil, errors.New("Must provide a keyfile when providing a certfile")
//		}
//
//		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	return &tls.Config{
//		MinVersion:         tls.VersionTLS10,
//		MaxVersion:         tls.VersionTLS12,
//		RootCAs:            caCertPool,
//		Certificates:       []tls.Certificate{cert},
//		InsecureSkipVerify: tlsNoVerify,
//	}, nil
//}

type boltConn struct {
	options       *DriverOptions
	url           *url.URL
	user          string
	password      string
	conn          net.Conn
	connErr       error
	serverVersion []byte
	closed        bool

	pool *connPool
	mu   sync.RWMutex

	transaction *boltTx
	statement   *boltStmt

	//openedAt time.Time
	//usedAt   time.Time
	//driver     *boltDriver
	//poolDriver DriverPool
}

// initialize attempts to actually connect to the Neo4j instance, optionally
// incorporating initialization of the recorder.
func (c *boltConn) initialize(d *boltDriver) (err error) {
	// Handle recorder. If there is no conn string, assume we're playing back a
	// recording.  If there is a recorder and a conn string, assume we're
	// recording the connection Else, just create the conn normally
	if c.options.Addr == "" && d != nil && d.recorder != nil {
		c.conn = d.recorder
	} else if d != nil && d.recorder != nil {
		d.recorder.Conn, err = c.connect()
		if err != nil {
			return err
		}
		c.conn = d.recorder
	} else {
		c.conn, err = c.connect()
		if err != nil {
			return err
		}
	}

	// do neo4j handshake
	if err := c.handShake(); err != nil {
		if e := c.Close(); e != nil {
			log.Errorf("An error occurred initializing connection: %v", e)
		}
		return err
	}

	respInt, err := c.sendInit()
	if err != nil {
		if e := c.Close(); e != nil {
			log.Errorf("An error occurred closing connection: %s", e)
		}
		return err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Successfully initiated Bolt connection: %+v", resp)
		return nil
	default:
		log.Errorf("Got an unrecognized message when initializing connection :%+v", resp)
		c.connErr = errors.New("Unrecognized response from the server: %#v", resp)
		c.Close()
		return driver.ErrBadConn
	}
}

func (c *boltConn) connect() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", c.url.Host, c.options.DialTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred dialing to neo4j")
	}

	if c.options.TLSConfig != nil {
		conn := tls.Client(conn, c.options.TLSConfig)
		err = conn.Handshake()
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred dialing to neo4j")
		}
	}

	return conn, nil
}

// handShake performs the Neo4J bolt handshake protocol, returning an error if
// the server doesn't respond correctly
func (c *boltConn) handShake() error {

	numWritten, err := c.Write(handShake)
	if numWritten != 20 {
		log.Errorf("Couldn't write expected bytes for magic preamble + supported versions. Written: %d. Expected: 4", numWritten)
		if err != nil {
			err = errors.Wrap(err, "An error occurred writing magic preamble + supported versions")
		}
		return err
	}

	numRead, err := c.Read(c.serverVersion)
	if numRead != 4 {
		log.Errorf("Could not read server version response. Read %d bytes. Expected 4 bytes. Output: %s", numRead, c.serverVersion)
		if err != nil {
			err = errors.Wrap(err, "An error occurred reading server version")
		}
		return err
	} else if bytes.Equal(c.serverVersion, noVersionSupported) {
		return errors.New("Server responded with no supported version")
	}

	return nil
}

// Read reads the data from the underlying connection
func (c *boltConn) Read(b []byte) (n int, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.options.ReadTimeout)); err != nil {
		c.connErr = errors.Wrap(err, "An error occurred setting read deadline")
		return 0, driver.ErrBadConn
	}

	n, err = c.conn.Read(b)

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Read %d bytes from stream:\n\n%s\n", n, sprintByteHex(b))
	}

	if err != nil && err != io.EOF {
		c.connErr = errors.Wrap(err, "An error occurred reading from stream")
		err = driver.ErrBadConn
	}
	return n, err
}

// Write writes the data to the underlying connection
func (c *boltConn) Write(b []byte) (n int, err error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.options.WriteTimeout)); err != nil {
		c.connErr = errors.Wrap(err, "An error occurred setting write deadline")
		return 0, driver.ErrBadConn
	}

	n, err = c.conn.Write(b)

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Wrote %d of %d bytes to stream:\n\n%s\n", len(b), n, sprintByteHex(b[:n]))
	}

	if err != nil {
		c.connErr = errors.Wrap(err, "An error occurred writing to stream")
		err = driver.ErrBadConn
	}
	return n, err
}

// Close attempts to put the connection back into the pool instead of closing it
func (c *boltConn) Close() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		log.Info("conn is already closed")
		return nil
	}

	if c.statement != nil {
		log.Info("closing statement")

		if err := c.statement.Close(); err != nil {
			return errors.Wrap(err, "Error closing statement when closing connection")
		}
	}

	if c.transaction != nil {
		log.Info("closing transaction")

		if err := c.transaction.Rollback(); err != nil {
			return errors.Wrap(err, "Error rolling back transaction when closing connection")
		}
	}

	// if we have a pool and there is no error on the connection, put back into
	// the pool
	if c.pool != nil && c.connErr == nil {
		log.Info("we have a pool, so try to return")
		err := c.pool.put(c)
		if err != nil {
			log.Errorf("An error occurred returning connection to pool: %s", err)
			c.connErr = errors.Wrap(err, "An error occurred closing the connection")
			return driver.ErrBadConn
		}
		return nil
	}

	err := c.conn.Close()
	c.closed = true
	if err != nil {
		// mark connection as bad
		c.connErr = errors.Wrap(err, "An error occurred closing the connection")
		return driver.ErrBadConn
	}

	return nil
}

func (c *boltConn) ackFailure(failure messages.FailureMessage) error {
	log.Infof("Acknowledging Failure: %#v", failure)

	ack := messages.NewAckFailureMessage()
	err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(ack)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding ack failure message")
	}

	for {
		respInt, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return errors.Wrap(err, "An error occurred decoding ack failure message response")
		}

		switch resp := respInt.(type) {
		case messages.IgnoredMessage:
			log.Infof("Got ignored message when acking failure: %#v", resp)
			continue
		case messages.SuccessMessage:
			log.Infof("Got success message when acking failure: %#v", resp)
			return nil
		case messages.FailureMessage:
			log.Errorf("Got failure message when acking failure: %#v", resp)
			return c.reset()
		default:
			log.Errorf("Got unrecognized response from acking failure: %#v", resp)
			c.connErr = errors.New("Got unrecognized response from acking failure: %#v. CLOSING SESSION!", resp)
			c.Close()
			return driver.ErrBadConn
		}
	}
}

func (c *boltConn) reset() error {
	log.Info("Resetting session")

	reset := messages.NewResetMessage()
	err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(reset)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding reset message")
	}

	for {
		respInt, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return errors.Wrap(err, "An error occurred decoding reset message response")
		}

		switch resp := respInt.(type) {
		case messages.IgnoredMessage:
			log.Infof("Got ignored message when resetting session: %#v", resp)
			continue
		case messages.SuccessMessage:
			log.Infof("Got success message when resetting session: %#v", resp)
			return nil
		case messages.FailureMessage:
			log.Errorf("Got failure message when resetting session: %#v", resp)
			err = c.Close()
			if err != nil {
				log.Errorf("An error occurred closing the session: %s", err)
			}
			return errors.Wrap(resp, "Error resetting session. CLOSING SESSION!")
		default:
			log.Errorf("Got unrecognized response from resetting session: %#v", resp)
			c.connErr = errors.New("Got unrecognized response from resetting session: %#v. CLOSING SESSION!", resp)
			c.Close()
			return driver.ErrBadConn
		}
	}
}

// Prepare prepares a new statement for a query
func (c *boltConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Prepare prepares a new statement for a query. Implements a Neo-friendly alternative to sql/driver.
func (c *boltConn) PrepareNeo(query string) (Stmt, error) {
	return c.prepare(query)
}

// PreparePipeline prepares a new pipeline statement for a query.
func (c *boltConn) PreparePipeline(queries ...string) (PipelineStmt, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}
	c.statement = newPipelineStmt(queries, c)
	return c.statement, nil
}

func (c *boltConn) prepare(query string) (*boltStmt, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}
	c.statement = newStmt(query, c)
	return c.statement, nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *boltConn) Begin() (driver.Tx, error) {
	if c.transaction != nil {
		return nil, errors.New("An open transaction already exists")
	}
	if c.statement != nil {
		return nil, errors.New("Cannot open a transaction when you already have an open statement")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	successInt, pullInt, err := c.sendRunPullAllConsumeSingle("BEGIN", nil)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred beginning transaction")
	}

	success, ok := successInt.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response type beginning transaction: %#v", success)
	}

	log.Infof("Got success message beginning transaction: %#v", success)

	success, ok = pullInt.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response type pulling transaction:  %#v", success)
	}

	log.Infof("Got success message pulling transaction: %#v", success)

	return newTx(c), nil
}

func (c *boltConn) consume() (interface{}, error) {
	log.Info("Consuming response from bolt stream")

	respInt, err := encoding.NewDecoder(c).Decode()
	if err != nil {
		return respInt, err
	}

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Consumed Response: %#v", respInt)
	}

	if failure, isFail := respInt.(messages.FailureMessage); isFail {
		log.Errorf("Got failure message: %#v", failure)
		err := c.ackFailure(failure)
		if err != nil {
			return nil, err
		}
		return failure, errors.Wrap(failure, "Neo4J reported a failure for the query")
	}

	return respInt, err
}

func (c *boltConn) consumeAll() ([]interface{}, interface{}, error) {
	log.Info("Consuming all responses until success/failure")

	responses := []interface{}{}
	for {
		respInt, err := c.consume()
		if err != nil {
			return nil, respInt, err
		}

		if success, isSuccess := respInt.(messages.SuccessMessage); isSuccess {
			log.Infof("Got success message: %#v", success)
			return responses, success, nil
		}

		responses = append(responses, respInt)
	}
}

func (c *boltConn) consumeAllMultiple(mult int) ([][]interface{}, []interface{}, error) {
	log.Info("Consuming all responses %d times until success/failure", mult)

	responses := make([][]interface{}, mult)
	successes := make([]interface{}, mult)
	for i := 0; i < mult; i++ {

		resp, success, err := c.consumeAll()
		if err != nil {
			return responses, successes, err
		}

		responses[i] = resp
		successes[i] = success
	}

	return responses, successes, nil
}

// sendInit attempts to send authentication credentials to the Neo4j server,
// returning any error.
func (c *boltConn) sendInit() (interface{}, error) {
	log.Infof("Sending INIT Message. ClientID: %s User: %s Password: <redacted>", ClientID, c.user)

	initMessage := messages.NewInitMessage(ClientID, c.user, c.password)
	if err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(initMessage); err != nil {
		return nil, errors.Wrap(err, "An error occurred sending init message")
	}

	return c.consume()
}

func (c *boltConn) sendRun(query string, args map[string]interface{}) error {
	log.Infof("Sending RUN message: query %s (args: %#v)", query, args)
	runMessage := messages.NewRunMessage(query, args)
	if err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(runMessage); err != nil {
		return errors.Wrap(err, "An error occurred running query")
	}

	return nil
}

func (c *boltConn) sendRunConsume(query string, args map[string]interface{}) (interface{}, error) {
	if err := c.sendRun(query, args); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendPullAll() error {
	log.Infof("Sending PULL_ALL message")

	pullAllMessage := messages.NewPullAllMessage()
	err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(pullAllMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding pull all query")
	}

	return nil
}

func (c *boltConn) sendPullAllConsume() (interface{}, error) {
	if err := c.sendPullAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunPullAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}

	return c.sendPullAll()
}

func (c *boltConn) sendRunPullAllConsumeRun(query string, args map[string]interface{}) (interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunPullAllConsumeSingle(query string, args map[string]interface{}) (interface{}, interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, nil, err
	}

	runSuccess, err := c.consume()
	if err != nil {
		return runSuccess, nil, err
	}

	pullSuccess, err := c.consume()
	return runSuccess, pullSuccess, err
}

func (c *boltConn) sendRunPullAllConsumeAll(query string, args map[string]interface{}) (interface{}, interface{}, []interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, nil, nil, err
	}

	runSuccess, err := c.consume()
	if err != nil {
		return runSuccess, nil, nil, err
	}

	records, pullSuccess, err := c.consumeAll()
	return runSuccess, pullSuccess, records, err
}

func (c *boltConn) sendDiscardAll() error {
	log.Infof("Sending DISCARD_ALL message")

	discardAllMessage := messages.NewDiscardAllMessage()
	err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(discardAllMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding discard all query")
	}

	return nil
}

func (c *boltConn) sendDiscardAllConsume() (interface{}, error) {
	if err := c.sendDiscardAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunDiscardAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}

	return c.sendDiscardAll()
}

func (c *boltConn) sendRunDiscardAllConsume(query string, args map[string]interface{}) (interface{}, interface{}, error) {
	runResp, err := c.sendRunConsume(query, args)
	if err != nil {
		return runResp, nil, err
	}

	discardResp, err := c.sendDiscardAllConsume()
	return runResp, discardResp, err
}

func (c *boltConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	return c.queryNeo(query, params)
}

func (c *boltConn) QueryNeo(query string, params map[string]interface{}) (Rows, error) {
	return c.queryNeo(query, params)
}

func (c *boltConn) QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error) {
	rows, err := c.queryNeo(query, params)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	data, metadata, err := rows.All()
	return data, rows.metadata, metadata, err
}

func (c *boltConn) queryNeo(query string, params map[string]interface{}) (*boltRows, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	c.statement = newStmt(query, c)

	// Pipeline the run + pull all for this
	successResp, err := c.sendRunPullAllConsumeRun(c.statement.query, params)
	if err != nil {
		return nil, err
	}
	success, ok := successResp.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unexpected response querying neo from connection: %#v", successResp)
	}

	c.statement.rows = newQueryRows(c.statement, success.Metadata)
	return c.statement.rows, nil
}

func (c *boltConn) QueryPipeline(queries []string, params ...map[string]interface{}) (PipelineRows, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	c.statement = newPipelineStmt(queries, c)
	rows, err := c.statement.QueryPipeline(params...)
	if err != nil {
		return nil, err
	}

	// Since we're not exposing the statement,
	// tell the rows to close it when they are closed
	rows.(*boltRows).closeStatement = true
	return rows, nil
}

// Exec executes a query that returns no rows. See sql/driver.Stmt.
// You must bolt encode a map to pass as []bytes for the driver value
func (c *boltConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newStmt(query, c)
	defer stmt.Close()

	return stmt.Exec(args)
}

// ExecNeo executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
func (c *boltConn) ExecNeo(query string, params map[string]interface{}) (Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newStmt(query, c)
	defer stmt.Close()

	return stmt.ExecNeo(params)
}

func (c *boltConn) ExecPipeline(queries []string, params ...map[string]interface{}) ([]Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newPipelineStmt(queries, c)
	defer stmt.Close()

	return stmt.ExecPipeline(params...)
}
