package main

import (
	"fmt"
	"os"

	"github.com/autovia/git-lfs-transfer/internal"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Print(help())
		fmt.Fprintf(os.Stderr, "fatal: expected 2 arguments, got %d\n", len(args)-1)
		os.Exit(1)
	}

	if args[2] != "upload" && args[2] != "download" {
		fmt.Print(help())
		fmt.Fprintf(os.Stderr, "fatal: unknown operation\n")
		os.Exit(1)
	}

	errc := make(chan error, 1)

	go func() {
		errc <- internal.Transfer(os.Stdin, os.Stdout, args)
	}()

	err := <-errc
	if err != nil {
		fmt.Print(help())
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func help() string {
	return `git-lfs-transfer - Server-side implementation of Git LFS over SSH

usage: git-lfs-transfer <git-dir> <operation>

`
}
