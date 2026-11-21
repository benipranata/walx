package main

import (
	"fmt"
	"io"
)

const usage = "usage: walx snapshot [-database name]"

// run dispatches args to a subcommand and returns the process exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprintln(stdout, usage)
		return 0
	case "snapshot":
		return runSnapshot(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "walx: unknown command %q\n%s\n", args[0], usage)
		return 2
	}
}
