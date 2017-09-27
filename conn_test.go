package neo4jbolt

import (
	"crypto/tls"
	"io"
	"net/url"
	"reflect"
	"testing"

	"github.com/pkg/errors"

	"github.com/thingful/golang-neo4j-bolt-driver/structures/messages"
)

func TestBoltConn_InvalidURLs(t *testing.T) {
	testcases := []struct {
		label string
		addr  string
	}{
		{
			label: "non bolt scheme",
			addr:  "http://foo:7687",
		},
		{
			label: "missing password when user present",
			addr:  "bolt://john@foo:7687",
		},
		{
			label: "invalid url",
			addr:  "://foo:7687",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.label, func(t *testing.T) {
			driver := NewDriver()
			_, err := driver.Open(testcase.addr)
			if err == nil {
				t.Error("expected error from incorrect protocol")
			}
		})
	}
}

func TestBoltConn_parseTLSConfig(t *testing.T) {
	testcases := []struct {
		label          string
		rawURL         string
		expectedConfig *tls.Config
	}{
		{
			label:          "non tls config present",
			rawURL:         "bolt://localhost:7687",
			expectedConfig: nil,
		},
		{
			label:  "tls and no verify",
			rawURL: "bolt://localhost:7687?tls=true&tls_no_verify=true",
			expectedConfig: &tls.Config{
				MinVersion:         tls.VersionTLS10,
				MaxVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
				ServerName:         "localhost",
			},
		},
		{
			label:  "tls and no verify as numbers",
			rawURL: "bolt://localhost:7687?tls=1&tls_no_verify=1",
			expectedConfig: &tls.Config{
				MinVersion:         tls.VersionTLS10,
				MaxVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
				ServerName:         "localhost",
			},
		},
		{
			label:  "tls and verify",
			rawURL: "bolt://localhost:7687?tls=true&tls_no_verify=false",
			expectedConfig: &tls.Config{
				MinVersion:         tls.VersionTLS10,
				MaxVersion:         tls.VersionTLS12,
				InsecureSkipVerify: false,
				ServerName:         "localhost",
			},
		},
	}

	for _, testcase := range testcases {
		u, err := url.Parse(testcase.rawURL)
		if err != nil {
			t.Errorf("error parsing url: %v", err)
		}

		got, err := parseTLSConfig(u)
		if err != nil {
			t.Errorf("unexpected error parsing tls config: %v", err)
		}

		if !reflect.DeepEqual(got, testcase.expectedConfig) {
			t.Fatalf("Unexpected TLS config. Expected %#v. Got: %#v", testcase.expectedConfig, got)
		}
	}
}

func TestBoltConn_Close(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_Close", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("An error occurred closing conn: %s", err)
	}

	if !conn.(*boltConn).closed {
		t.Error("Conn not closed at end of test")
	}
}

func TestBoltConn_SelectOne(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_SelectOne", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	rows, err := conn.QueryNeo("RETURN 1;", nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	expectedMetadata := map[string]interface{}{
		"result_available_after": rows.Metadata()["result_available_after"],
		"fields":                 []interface{}{"1"},
	}

	if !reflect.DeepEqual(rows.Metadata(), expectedMetadata) {
		t.Fatalf("Unexpected success metadata. Expected %#v. Got: %#v", expectedMetadata, rows.Metadata())
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(int64) != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %d", output)
	}

	_, metadata, err := rows.NextNeo()
	expectedMetadata = map[string]interface{}{
		"result_consumed_after": metadata["result_consumed_after"],
		"type":                  "r",
	}

	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	} else if !reflect.DeepEqual(metadata, expectedMetadata) {
		t.Fatalf("Metadata didn't match expected. Expected %#v. Got: %#v", expectedMetadata, metadata)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltConn_SelectAll(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_SelectAll", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	results, err := conn.ExecNeo("CREATE (f:NODE {a: 1}), (b:NODE {a: 2})", nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}
	affected, err := results.RowsAffected()
	if err != nil {
		t.Fatalf("An error occurred getting rows affected: %s", err)
	}
	if affected != int64(2) {
		t.Fatalf("Incorrect number of rows affected: %d", affected)
	}

	data, rowMetadata, metadata, err := conn.QueryNeoAll("MATCH (n:NODE) RETURN n.a ORDER BY n.a", nil)
	if data[0][0] != int64(1) {
		t.Fatalf("Incorrect data returned for first row: %#v", data[0])
	}
	if data[1][0] != int64(2) {
		t.Fatalf("Incorrect data returned for second row: %#v", data[1])
	}

	if rowMetadata["fields"].([]interface{})[0] != "n.a" {
		t.Fatalf("Unexpected column metadata: %#v", rowMetadata)
	}

	if metadata["type"].(string) != "r" {
		t.Fatalf("Unexpected request metadata: %#v", metadata)
	}

	results, err = conn.ExecNeo("MATCH (n:NODE) DELETE n", nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}
	affected, err = results.RowsAffected()
	if err != nil {
		t.Fatalf("An error occurred getting rows affected: %s", err)
	}
	if affected != int64(2) {
		t.Fatalf("Incorrect number of rows affected: %d", affected)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltConn_Ignored(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_Ignored", neo4jConnStr)

	conn, _ := driver.OpenNeo(neo4jConnStr)
	defer conn.Close()

	// This will make two calls at once - Run and Pull All.  The pull all should be ignored, which is what
	// we're testing.
	_, err := conn.ExecNeo("syntax error", map[string]interface{}{"foo": 1, "bar": 2.2})
	if err == nil {
		t.Fatal("Expected an error on syntax error.")
	}

	data, _, _, err := conn.QueryNeoAll("RETURN 1;", nil)
	if err != nil {
		t.Fatalf("Got error when running next query after a failure: %#v", err)
	}

	if data[0][0].(int64) != 1 {
		t.Fatalf("Expected different data from output: %#v", data)
	}
}

func TestBoltConn_IgnoredPipeline(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_IgnoredPipeline", neo4jConnStr)

	conn, _ := driver.OpenNeo(neo4jConnStr)
	defer conn.Close()

	// This will make two calls at once - Run and Pull All.  The pull all should be ignored, which is what
	// we're testing.
	_, err := conn.ExecPipeline([]string{"syntax error", "syntax error", "syntax error"}, nil)
	if err == nil {
		t.Fatal("Expected an error on syntax error.")
	}

	data, _, _, err := conn.QueryNeoAll("RETURN 1;", nil)
	if err != nil {
		t.Fatalf("Got error when running next query after a failure: %#v", err)
	}

	if data[0][0].(int64) != 1 {
		t.Fatalf("Expected different data from output: %#v", data)
	}
}

func TestBoltConn_FailureMessageError(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_FailureMessageError", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	defer conn.Close()
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	_, err = conn.ExecNeo("THIS IS A BAD QUERY AND SHOULD RETURN A FAILURE MESSAGE", nil)
	if err == nil {
		t.Fatal("This should have returned a failure message error, but got a nil error")
	}

	code := "Neo.ClientError.Statement.SyntaxError"
	ex, ok := errors.Cause(err).(messages.FailureMessage)
	if ok {
		if ex.Metadata["code"] != code {
			t.Fatalf("Expected error message code %s, but got %s", code, ex.Metadata["code"])
		}
	} else {
		t.Fatalf("Unexpected error type: %v", err)
	}
}
