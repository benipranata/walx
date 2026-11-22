package postgres

import (
	"slices"

	"github.com/benipranata/walx/internal/config"
)

// columnRef is a column named by one config field of a table.
type columnRef struct {
	Field  string
	Column string
}

// columnRefs lists every column a table's config names in key, where,
// on_change, fsm_column and drop_columns, in a stable order. publication_columns
// is left out because Postgres rejects an unknown column itself.
func columnRefs(t config.TableConfig) []columnRef {
	var refs []columnRef
	add := func(field string, cols ...string) {
		for _, c := range cols {
			refs = append(refs, columnRef{field, c})
		}
	}
	add("key", t.Key...)
	wheres := make([]string, 0, len(t.Where))
	for c := range t.Where {
		wheres = append(wheres, c)
	}
	slices.Sort(wheres)
	add("where", wheres...)
	add("on_change", t.OnChange...)
	if t.FSMColumn != "" {
		add("fsm_column", t.FSMColumn)
	}
	add("drop_columns", t.DropColumns...)
	return refs
}
