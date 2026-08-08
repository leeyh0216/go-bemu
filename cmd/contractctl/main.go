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
	if len(arguments) == 0 || (arguments[0] != "compile" && arguments[0] != "check" && arguments[0] != "matrix") {
		return fmt.Errorf("usage: contractctl <compile|check|matrix> [--root directory] [--family name] [--lane name]")
	}
	command := arguments[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	root := flags.String("root", ".", "repository root")
	family := flags.String("family", "", "consumer family filter (matrix only)")
	lane := flags.String("lane", "", "consumer lane filter (matrix only)")
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
	case "matrix":
		matrix, err := contract.ConsumerMatrix(absolute, *family, *lane)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(append(matrix, '\n'))
		return err
	default:
		panic("unreachable")
	}
}
