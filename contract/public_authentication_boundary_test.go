package contract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicAuthenticationEnforcementDoesNotRegress(t *testing.T) {
	root := filepath.Clean("..")
	removedPaths := []string{
		"cmd/emulator/auth.go",
		"internal/transport/rest/auth.go",
		"internal/transport/grpc/auth.go",
	}
	for _, relative := range removedPaths {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(relative))); !os.IsNotExist(err) {
			t.Errorf("public authentication runtime path %s must remain removed, stat error = %v", relative, err)
		}
	}

	for relative, forbidden := range map[string][]string{
		"cmd/emulator/main.go":              {"composeAuthentication", "WithAuthentication", "Authentication:"},
		"internal/config/config.go":         {"AuthConfig", "BQEMU_AUTH_", `"auth.mode"`},
		"internal/transport/rest/server.go": {"AuthenticationUseCases", "authenticationMiddleware"},
		"internal/transport/grpc/server.go": {"authenticationUnaryServerInterceptor", "authenticationStreamServerInterceptor"},
	} {
		contents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("read %s: %v", relative, err)
		}
		for _, marker := range forbidden {
			if strings.Contains(string(contents), marker) {
				t.Errorf("%s reintroduced public authentication marker %q", relative, marker)
			}
		}
	}

	admin, err := os.ReadFile(filepath.Join(root, "internal/admin/server.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Authorization", "Bearer ", "subtle.ConstantTimeCompare"} {
		if !strings.Contains(string(admin), required) {
			t.Errorf("admin token protection lost marker %q", required)
		}
	}
}
