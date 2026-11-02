package main

import (
	"fmt"
	"nse-mtbt-handler/internal/tickerapp"
	"os"
)

func main() {
	if e := tickerapp.Run(os.Args[1:], false); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
