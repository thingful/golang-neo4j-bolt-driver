package neo4jbolt

// Error is type alias for string allowing us to create const Error instances
type Error string

// Error is our function definition to adhere to the Error interface
func (e Error) Error() string { return string(e) }

const (
	// ErrInvalidCapacity is returned if a pool is requested with invalid
	// capacity, i.e. a capacity of less than zero, or max capacity less than the
	// initial capacity
	ErrInvalidCapacity = Error("invalid capacity settings")

	// ErrClosed is returned if we attempt to get a connection from a closed pool
	ErrClosed = Error("pool is closed")
)
