package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/leeyh0216/go-bemu/contract"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 || (arguments[0] != "compile" && arguments[0] != "check") {
		return fmt.Errorf("usage: contractctl <compile|check> [--root directory]")
	}
	command := arguments[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	root := flags.String("root", ".", "repository root")
	if err := flags.Parse(arguments[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	absolute, err := filepath.Abs(*root)
	if err != nil {
		return err
	}
	switch command {
	case "compile":
		return contract.WriteOperationArtifacts(absolute)
	case "check":
		return contract.CheckOperationArtifacts(absolute)
	default:
		panic("unreachable")
	}
}
