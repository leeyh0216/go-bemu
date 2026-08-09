package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	localauth "github.com/leeyh0216/go-bemu/internal/auth/issuer/local"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bqemu-auth-fixture:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return errors.New("usage: bqemu-auth-fixture <generate|serve> [options]")
	}
	switch arguments[0] {
	case "generate":
		flags := flag.NewFlagSet("generate", flag.ContinueOnError)
		output := flags.String("output", ".bqemu-auth", "output directory")
		baseURL := flags.String("base-url", "https://localhost:9052", "local issuer HTTPS origin")
		force := flags.Bool("force", false, "replace an existing fixture")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("generate does not accept positional arguments")
		}
		manifest, err := localauth.Generate(localauth.GenerateOptions{OutputDir: *output, BaseURL: *baseURL, Force: *force})
		if err != nil {
			return err
		}
		fmt.Println(filepath.Join(filepath.Dir(manifest.CACertificate), "manifest.json"))
		return nil
	case "serve":
		flags := flag.NewFlagSet("serve", flag.ContinueOnError)
		manifestPath := flags.String("manifest", ".bqemu-auth/manifest.json", "generated manifest path")
		address := flags.String("listen", "127.0.0.1:9052", "loopback listen address")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("serve does not accept positional arguments")
		}
		logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
		issuer, err := localauth.NewServer(*manifestPath, logger)
		if err != nil {
			return err
		}
		server, err := issuer.HTTPServer(*address)
		if err != nil {
			return err
		}
		stopContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		result := make(chan error, 1)
		go func() { result <- issuer.ServeTLS(server) }()
		select {
		case err := <-result:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-stopContext.Done():
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return server.Shutdown(ctx)
		}
	default:
		return fmt.Errorf("unknown command %q; use generate or serve", arguments[0])
	}
}
