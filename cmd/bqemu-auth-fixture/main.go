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
		proxyURL := flags.String("proxy-url", "http://127.0.0.1:9053", "loopback OAuth CONNECT proxy origin")
		keytool := flags.String("keytool", "keytool", "keytool executable used for the PKCS12 truststore")
		var tlsDNSNames stringList
		flags.Var(&tlsDNSNames, "tls-dns-name", "additional DNS name for the server certificate; repeatable")
		force := flags.Bool("force", false, "replace an existing fixture")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("generate does not accept positional arguments")
		}
		manifest, err := localauth.Generate(localauth.GenerateOptions{
			OutputDir: *output,
			BaseURL:   *baseURL,
			ProxyURL:  *proxyURL,
			Force:     *force,
			Keytool:   *keytool,
			DNSNames:  []string(tlsDNSNames),
		})
		if err != nil {
			return err
		}
		fmt.Println(filepath.Join(filepath.Dir(manifest.CACertificate), "manifest.json"))
		return nil
	case "serve":
		flags := flag.NewFlagSet("serve", flag.ContinueOnError)
		manifestPath := flags.String("manifest", ".bqemu-auth/manifest.json", "generated manifest path")
		address := flags.String("listen", "127.0.0.1:9052", "loopback listen address")
		proxyAddress := flags.String("proxy-listen", "", "loopback OAuth proxy address; defaults to the manifest")
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
		if *proxyAddress == "" {
			*proxyAddress, err = issuer.ProxyListenAddress()
			if err != nil {
				return err
			}
		}
		proxyServer, err := issuer.ProxyHTTPServer(*proxyAddress, *address)
		if err != nil {
			return err
		}
		stopContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		result := make(chan error, 2)
		go func() { result <- issuer.ServeTLS(server) }()
		go func() { result <- proxyServer.ListenAndServe() }()
		select {
		case err := <-result:
			shutdownErr := shutdownServers(server, proxyServer)
			if errors.Is(err, http.ErrServerClosed) {
				return shutdownErr
			}
			return errors.Join(err, shutdownErr)
		case <-stopContext.Done():
			return shutdownServers(server, proxyServer)
		}
	default:
		return fmt.Errorf("unknown command %q; use generate or serve", arguments[0])
	}
}

func shutdownServers(servers ...*http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var shutdownErrors []error
	for _, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
	}
	return errors.Join(shutdownErrors...)
}

type stringList []string

func (values *stringList) String() string {
	if values == nil {
		return ""
	}
	return fmt.Sprint([]string(*values))
}

func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}
