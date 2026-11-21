package event

type unchanged struct{}

// Unchanged marks a TOAST column the update did not rewrite, so its value is
// unknown rather than null.
var Unchanged = unchanged{}

// MarshalJSON renders the sentinel as {"$unchanged":true}.
func (unchanged) MarshalJSON() ([]byte, error) {
	return []byte(`{"$unchanged":true}`), nil
}
