package engine

import "github.com/benipranata/walx/internal/event"

// confirmEmpty confirms the EndLSN of a batch with nothing to send. It runs on
// the deliverer, after every earlier batch was sent and confirmed, so the
// confirmed LSN never passes undelivered data.
func (e *Engine) confirmEmpty(b event.Batch) {
	e.confirm(b.EndLSN)
	e.log.Debug("empty batch confirmed", "commit_lsn", b.CommitLSN.String(), "end_lsn", b.EndLSN.String())
}

// keepalive confirms the server WAL end when nothing is pending: no
// transaction is buffered and no batch is queued or in flight. It runs on the
// reader goroutine, the only producer of batches, so pending cannot grow
// between the check and the confirm. Keepalives arrive after every earlier
// message was decoded, because the reader handles them in the same loop.
func (e *Engine) keepalive(walEnd event.LSN) {
	if e.pending.Load() != 0 || (e.cfg.Buffer != nil && e.cfg.Buffer.InTx()) {
		return
	}
	e.confirm(walEnd)
	e.log.Debug("idle advance", "wal_end", walEnd.String())
}
