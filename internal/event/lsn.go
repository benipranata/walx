package event

import (
	"encoding/json"
	"fmt"
)

// LSN is a Postgres write-ahead log position.
type LSN uint64

// String formats the LSN the way Postgres does, as two hex halves.
func (l LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(l>>32), uint32(l))
}

// MarshalJSON encodes the LSN as its string form. A JSON number would lose
// precision above 2^53 in JavaScript consumers.
func (l LSN) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.String())
}
