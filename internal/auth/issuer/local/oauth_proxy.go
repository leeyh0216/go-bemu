package local

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	maxProxyConnections = 32
	proxyTunnelLifetime = 2 * time.Minute
)

var allowedOAuthProxyTargets = map[string]struct{}{
	"accounts.google.com:443":   {},
	"oauth2.googleapis.com:443": {},
}

type oauthConnectProxy struct {
	upstream string
	logger   *slog.Logger
	slots    chan struct{}
}

func (s *Server) ProxyListenAddress() (string, error) {
	parsed, err := url.Parse(s.manifest.OAuthProxyURL)
	if err != nil || parsed.Host == "" {
		return "", errors.New("manifest OAuth proxy URL is invalid")
	}
	return addressWithDefaultPort(parsed, "80"), nil
}

func (s *Server) ProxyHTTPServer(address, issuerAddress string) (*http.Server, error) {
	if !validLoopbackAddress(address) || !validLoopbackAddress(issuerAddress) {
		return nil, errors.New("OAuth proxy and issuer addresses must be loopback host and port pairs")
	}
	if err := proxyAddressMismatch(address, s.manifest.OAuthProxyURL); err != nil {
		return nil, err
	}
	if err := issuerAddressMismatch(issuerAddress, s.manifest.BaseURL); err != nil {
		return nil, err
	}
	proxy := &oauthConnectProxy{
		upstream: issuerAddress,
		logger:   s.logger,
		slots:    make(chan struct{}, maxProxyConnections),
	}
	return &http.Server{
		Addr:              address,
		Handler:           proxy,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}, nil
}

func (p *oauthConnectProxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	status := http.StatusMethodNotAllowed
	targetAllowed := request.Method == http.MethodConnect && allowedOAuthTarget(request.Host)
	defer func() {
		p.logger.Info(
			"local OAuth proxy request",
			"method", request.Method,
			"target", request.Host,
			"headers", request.Header,
			"target_allowed", targetAllowed,
			"status", status,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}()

	if !targetAllowed {
		http.Error(writer, "proxy target is not allowed", status)
		return
	}
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	default:
		status = http.StatusServiceUnavailable
		http.Error(writer, "proxy connection capacity reached", status)
		return
	}

	upstream, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		status = http.StatusBadGateway
		http.Error(writer, "local OAuth issuer is unavailable", status)
		return
	}
	defer upstream.Close()

	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		status = http.StatusInternalServerError
		http.Error(writer, "HTTP server does not support CONNECT", status)
		return
	}
	client, buffered, err := hijacker.Hijack()
	if err != nil {
		status = http.StatusInternalServerError
		return
	}
	defer client.Close()
	deadline := time.Now().Add(proxyTunnelLifetime)
	client.SetDeadline(deadline)
	upstream.SetDeadline(deadline)
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}
	status = http.StatusOK
	proxyBidirectional(client, buffered, upstream)
}

func proxyBidirectional(client net.Conn, buffered *bufio.ReadWriter, upstream net.Conn) {
	completed := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, buffered)
		if tcp, ok := upstream.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		completed <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, upstream)
		if tcp, ok := client.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		completed <- struct{}{}
	}()
	<-completed
}

func allowedOAuthTarget(target string) bool {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	normalized := strings.ToLower(net.JoinHostPort(strings.TrimSuffix(host, "."), port))
	_, allowed := allowedOAuthProxyTargets[normalized]
	return allowed
}

func validLoopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	return err == nil && port != "" && loopbackHost(host)
}

func addressWithDefaultPort(parsed *url.URL, fallbackPort string) string {
	port := parsed.Port()
	if port == "" {
		port = fallbackPort
	}
	return net.JoinHostPort(parsed.Hostname(), port)
}

func proxyAddressMismatch(configured, manifestURL string) error {
	return addressPortMismatch(configured, manifestURL, "OAuth proxy", "80")
}

func issuerAddressMismatch(configured, manifestURL string) error {
	return addressPortMismatch(configured, manifestURL, "OAuth issuer", "443")
}

func addressPortMismatch(configured, manifestURL, component, defaultPort string) error {
	parsed, err := url.Parse(manifestURL)
	if err != nil {
		return fmt.Errorf("manifest %s URL is invalid", component)
	}
	expected := addressWithDefaultPort(parsed, defaultPort)
	_, configuredPort, configuredErr := net.SplitHostPort(configured)
	_, expectedPort, expectedErr := net.SplitHostPort(expected)
	if configuredErr != nil || expectedErr != nil || configuredPort != expectedPort {
		return fmt.Errorf("%s listen port must match generated manifest port %s", component, expectedPort)
	}
	return nil
}
