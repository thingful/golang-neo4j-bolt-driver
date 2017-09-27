package neo4jbolt_test

import (
	"fmt"
	"os"

	bolt "github.com/thingful/golang-neo4j-bolt-driver"
)

func mustGetConnStr() string {
	connStr := os.Getenv("NEO4J_BOLT")
	if connStr == "" {
		panic("Must set connection URL via NEO4J_BOLT environment variable")
	}

	return connStr
}

func ExampleNewDriver_openNeo() {
	driver := bolt.NewDriver()

	connStr := mustGetConnStr()

	conn, err := driver.OpenNeo(connStr)
	if err != nil {
		panic(err)
	}

	rows, err := conn.QueryNeo("RETURN 1;", nil)
	if err != nil {
		panic(err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		panic(err)
	}

	fmt.Println(output[0])

	err = conn.Close()
	if err != nil {
		panic(err)
	}

	// Output: 1
}

func ExampleNewDriver_open() {

}
