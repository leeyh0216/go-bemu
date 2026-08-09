package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/leeyh0216/go-bemu/internal/release"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: releasectl prepare|check")
	}
	switch args[0] {
	case "prepare":
		return prepare(args[1:], output)
	case "check":
		return check(args[1:], output)
	default:
		return fmt.Errorf("usage: releasectl prepare|check")
	}
}

func prepare(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("prepare", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("file", "release/version.json", "canonical version descriptor")
	bump := flags.String("bump", "", "version bump")
	version := flags.String("version", "", "exact stable version")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if (*bump == "") == (*version == "") {
		return fmt.Errorf("choose exactly one of --bump or --version")
	}
	current, err := release.Read(*path)
	if err != nil {
		return err
	}
	next := release.Descriptor{Version: *version}
	if *bump != "" {
		next, err = current.Next(*bump)
		if err != nil {
			return err
		}
	}
	if err := next.Validate(); err != nil {
		return err
	}
	comparison, err := release.Compare(next, current)
	if err != nil {
		return err
	}
	if comparison <= 0 {
		return fmt.Errorf("version %s must be greater than current %s", next.Version, current.Version)
	}
	if err := release.Write(*path, next); err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, next.Version)
	return err
}

func check(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("file", "release/version.json", "canonical version descriptor")
	baseVersion := flags.String("base-version", "", "main-base stable version")
	if err := flags.Parse(args); err != nil {
		return err
	}
	current, err := release.Read(*path)
	if err != nil {
		return err
	}
	if *baseVersion != "" {
		comparison, err := release.Compare(current, release.Descriptor{Version: *baseVersion})
		if err != nil {
			return err
		}
		if comparison <= 0 {
			return fmt.Errorf("version %s must be greater than main base %s", current.Version, *baseVersion)
		}
	}
	_, err = fmt.Fprintln(output, current.Version)
	return err
}
