package main

import (
	"flag"
	"fmt"
	"io"
)

// runSnapshot is a placeholder that reserves the snapshot subcommand and its
// -database flag until stage 2.
func runSnapshot(args []string, _, stderr io.Writer) int {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.String("database", "", "name of the database entry to snapshot")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "walx snapshot: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	fmt.Fprintln(stderr, "walx snapshot: not implemented")
	return 1
}
