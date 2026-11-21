package event

// Batch is the unit a Sink makes durable. The engine confirms EndLSN only
// after Send returns nil.
type Batch struct {
	Events    []Event
	CommitLSN LSN
	EndLSN    LSN
}
