// Command hopcorder is a flight recorder for network paths.
//
// Usage:
//
//	hopcorder agent [flags] [target ...]
//	hopcorder server            (milestone 2)
//	hopcorder version
package main

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
)

// version is set at build time with -ldflags "-X main.version=v1.2.3".
var version = ""

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "agent":
		return runAgent(args[1:], stdout, stderr)
	case "server":
		fmt.Fprintln(stderr, "hopcorder server: not implemented yet (milestone 2)")
		return 1
	case "version", "-version", "--version":
		fmt.Fprintln(stdout, "hopcorder", versionString())
		return 0
	case "help", "-h", "-help", "--help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "hopcorder: unknown command %q\n\n", args[0])
		usage(stderr)
		return 2
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `hopcorder — a flight recorder for network paths

Usage:
  hopcorder agent [flags] [target ...]   run the probing agent
  hopcorder server                       run the ingest server (not yet implemented)
  hopcorder version                      print version information

Run "hopcorder agent -h" for agent flags.
`)
}

func versionString() string {
	v := version
	info, ok := debug.ReadBuildInfo()
	if v == "" && ok && info.Main.Version != "" {
		v = info.Main.Version
	}
	if v == "" {
		v = "(devel)"
	}
	if ok {
		var rev, dirty string
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.modified":
				if s.Value == "true" {
					dirty = "-dirty"
				}
			}
		}
		if len(rev) > 12 {
			rev = rev[:12]
		}
		if rev != "" {
			v += " (" + rev + dirty + ")"
		}
		v += " " + info.GoVersion
	}
	return v
}
