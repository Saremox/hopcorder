package main

import (
	"fmt"
	"io"
)

func runAgent(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "hopcorder agent: not implemented yet")
	return 1
}
