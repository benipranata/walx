package event

// Batch is the unit a Sink makes durable. The engine confirms EndLSN only
// after Send returns nil. Events are shared pointers: copy one before
// changing it.
type Batch struct {
	Events    []*Event
	CommitLSN LSN
	EndLSN    LSN
}
