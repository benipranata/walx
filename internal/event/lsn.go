package event

import "fmt"

// LSN is a Postgres write-ahead log position.
type LSN uint64

// String formats the LSN the way Postgres does, as two hex halves.
func (l LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(l>>32), uint32(l))
}
