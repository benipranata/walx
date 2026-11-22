package main

import (
	"fmt"
	"io"
	"strings"
)

// version is set at build time with -ldflags "-X main.version=v1.2.3".
var version = "dev"

const usage = "usage: walx [-config path] | walx version\n       walx snapshot [-config path] [-database name] [-table schema.table]... [-where expr] [-chunk n]"

// run dispatches args to a subcommand and returns the process exit code: 0 for
// a clean stop, 1 for a runtime or config error, 2 for a usage error.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		switch first := args[0]; {
		case first == "help" || first == "-h" || first == "--help":
			fmt.Fprintln(stdout, usage)
			return 0
		case first == "version" || first == "-version" || first == "--version":
			fmt.Fprintln(stdout, "walx "+version)
			return 0
		case first == "snapshot":
			return runSnapshot(args[1:], stdout, stderr)
		case !strings.HasPrefix(first, "-"):
			fmt.Fprintf(stderr, "walx: unknown command %q\n%s\n", first, usage)
			return 2
		}
	}
	return runServe(args, stderr)
}
