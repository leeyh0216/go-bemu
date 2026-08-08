package cipolicy

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var fullCommitSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// The owner is the Marketplace trust boundary. Keep this list deliberately
// small and update it only after checking the creator badge in Marketplace.
var approvedActionOwners = map[string]string{
	"actions":               "GitHub official",
	"astral-sh":             "Verified Creator",
	"docker":                "Verified Creator",
	"google-github-actions": "Verified Creator",
}

var requiredValidationJobs = []string{
	"static-validation",
	"go-smoke",
	"go-tests",
	"python-client",
	"bq-cli",
	"spark-contract",
	"auth-client-test",
	"container-smoke",
}

func TestExternalActionsUseApprovedOwnersAndFullSHAs(t *testing.T) {
	root := repositoryRoot(t)
	policyFiles := actionPolicyFiles(t, root)
	if len(policyFiles) == 0 {
		t.Fatal("no GitHub Actions workflows found")
	}

	for _, path := range policyFiles {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			document := loadWorkflow(t, path)
			walkUses(document, func(node *yaml.Node) {
				validateActionReference(t, path, node)
			})
		})
	}
}

func TestActionPolicyFilesIncludeBothWorkflowAndCompositeActionExtensions(t *testing.T) {
	root := t.TempDir()
	want := []string{
		filepath.Join(".github", "actions", "nested", "action.yaml"),
		filepath.Join(".github", "actions", "root", "action.yml"),
		filepath.Join(".github", "workflows", "one.yml"),
		filepath.Join(".github", "workflows", "two.yaml"),
	}
	for _, relativePath := range append(want, filepath.Join(".github", "actions", "ignored.yml")) {
		path := filepath.Join(root, relativePath)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("name: fixture\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	gotPaths := actionPolicyFiles(t, root)
	got := make([]string, 0, len(gotPaths))
	for _, path := range gotPaths {
		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, relativePath)
	}
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("policy files = %v, want %v", got, want)
	}
}

func actionPolicyFiles(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	for _, extension := range []string{"*.yml", "*.yaml"} {
		matches, err := filepath.Glob(filepath.Join(root, ".github", "workflows", extension))
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, matches...)
	}

	actionsRoot := filepath.Join(root, ".github", "actions")
	err := filepath.WalkDir(actionsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		if name == "action.yml" || name == "action.yaml" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	sort.Strings(paths)
	return paths
}

func TestPublishWorkflowIsReusableOnly(t *testing.T) {
	root := repositoryRoot(t)
	document := loadWorkflow(t, filepath.Join(root, ".github", "workflows", "publish-ghcr.yaml"))
	on := requiredMapping(t, document.Content[0], "on")
	keys := mappingKeys(t, on)
	if len(keys) != 1 || keys[0] != "workflow_call" {
		t.Fatalf("publish-ghcr.yaml triggers = %v, want only workflow_call", keys)
	}
}

func TestCIRequiresAllValidationBeforePublish(t *testing.T) {
	root := repositoryRoot(t)
	document := loadWorkflow(t, filepath.Join(root, ".github", "workflows", "ci.yaml"))
	workflow := document.Content[0]

	on := requiredMapping(t, workflow, "on")
	push := requiredMapping(t, on, "push")
	requireSequenceContains(t, requiredValue(t, push, "branches"), "main", "push branches")
	requireSequenceContains(t, requiredValue(t, push, "tags"), "v*", "push tags")

	jobs := requiredMapping(t, workflow, "jobs")
	authClient := requiredMapping(t, jobs, "auth-client-test")
	if !nodeContainsScalar(authClient, "make auth-client-test") {
		t.Fatal("auth-client-test job must use the stable Makefile entrypoint")
	}
	strategy := requiredMapping(t, authClient, "strategy")
	if got := requiredScalar(t, strategy, "fail-fast"); got != "false" {
		t.Fatalf("auth-client-test.strategy.fail-fast = %q, want false", got)
	}
	for _, consumerCase := range []string{"python", "bq", "pyspark", "scala-spark"} {
		if !nodeContainsScalar(authClient, consumerCase) {
			t.Errorf("auth-client-test matrix is missing %s", consumerCase)
		}
	}
	for _, contract := range []string{
		"BQEMU_AUTH_CASE",
		"BQEMU_AUTH_DIAGNOSTICS",
		"junit-$CONSUMER_CASE",
		"events.ndjson",
		"always()",
	} {
		if !nodeContainsScalar(authClient, contract) {
			t.Errorf("auth-client-test is missing matrix contract %q", contract)
		}
	}
	if nodeContainsScalar(authClient, "tee ") {
		t.Fatal("auth-client-test must not persist raw process output with tee")
	}
	aggregate := requiredMapping(t, jobs, "validation-complete")
	needs := sequenceValues(t, requiredValue(t, aggregate, "needs"))
	if diff := stringSetDifference(requiredValidationJobs, needs); len(diff) != 0 {
		t.Fatalf("validation-complete.needs is missing required jobs: %v", diff)
	}
	if condition := requiredScalar(t, aggregate, "if"); !strings.Contains(condition, "always()") {
		t.Fatalf("validation-complete.if = %q, want an always() result audit", condition)
	}
	for _, job := range requiredValidationJobs {
		needle := fmt.Sprintf("needs.%s.result", job)
		if !nodeContainsScalar(aggregate, needle) {
			t.Errorf("validation-complete does not audit %s", needle)
		}
	}

	publish := requiredMapping(t, jobs, "publish")
	if got := requiredScalar(t, publish, "needs"); got != "validation-complete" {
		t.Fatalf("publish.needs = %q, want validation-complete", got)
	}
	if got := requiredScalar(t, publish, "uses"); got != "./.github/workflows/publish-ghcr.yaml" {
		t.Fatalf("publish.uses = %q, want local publish workflow", got)
	}
	condition := requiredScalar(t, publish, "if")
	for _, required := range []string{"github.event_name == 'push'", "refs/heads/main", "refs/tags/v"} {
		if !strings.Contains(condition, required) {
			t.Errorf("publish.if = %q, missing %q", condition, required)
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve policy test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
}

func loadWorkflow(t *testing.T, path string) *yaml.Node {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(payload, &document); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		t.Fatalf("%s must contain one workflow mapping", path)
	}
	return &document
}

func walkUses(node *yaml.Node, visit func(*yaml.Node)) {
	if node.Kind == yaml.MappingNode {
		for index := 0; index < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if key.Value == "uses" {
				visit(value)
			}
			walkUses(value, visit)
		}
		return
	}
	for _, child := range node.Content {
		walkUses(child, visit)
	}
}

func validateActionReference(t *testing.T, path string, node *yaml.Node) {
	t.Helper()
	reference := node.Value
	if strings.HasPrefix(reference, "./") {
		return
	}
	parts := strings.Split(reference, "@")
	if len(parts) != 2 {
		t.Errorf("%s:%d: external uses %q must have exactly one @<commit SHA>", path, node.Line, reference)
		return
	}
	repository, revision := parts[0], parts[1]
	owner, _, found := strings.Cut(repository, "/")
	if !found {
		t.Errorf("%s:%d: external uses %q is not an owner/repository action", path, node.Line, reference)
		return
	}
	if _, approved := approvedActionOwners[owner]; !approved {
		owners := make([]string, 0, len(approvedActionOwners))
		for approvedOwner := range approvedActionOwners {
			owners = append(owners, approvedOwner)
		}
		sort.Strings(owners)
		t.Errorf("%s:%d: action owner %q is not approved; approved owners: %v", path, node.Line, owner, owners)
	}
	if !fullCommitSHA.MatchString(revision) {
		t.Errorf("%s:%d: external uses %q must be pinned to a lowercase 40-character commit SHA", path, node.Line, reference)
	}
}

func requiredValue(t *testing.T, mapping *yaml.Node, key string) *yaml.Node {
	t.Helper()
	if mapping.Kind != yaml.MappingNode {
		t.Fatalf("lookup %q in non-mapping YAML node", key)
	}
	for index := 0; index < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	t.Fatalf("required key %q is missing", key)
	return nil
}

func requiredMapping(t *testing.T, mapping *yaml.Node, key string) *yaml.Node {
	t.Helper()
	value := requiredValue(t, mapping, key)
	if value.Kind != yaml.MappingNode {
		t.Fatalf("%q must be a mapping", key)
	}
	return value
}

func requiredScalar(t *testing.T, mapping *yaml.Node, key string) string {
	t.Helper()
	value := requiredValue(t, mapping, key)
	if value.Kind != yaml.ScalarNode {
		t.Fatalf("%q must be a scalar", key)
	}
	return value.Value
}

func mappingKeys(t *testing.T, mapping *yaml.Node) []string {
	t.Helper()
	if mapping.Kind != yaml.MappingNode {
		t.Fatal("expected YAML mapping")
	}
	keys := make([]string, 0, len(mapping.Content)/2)
	for index := 0; index < len(mapping.Content); index += 2 {
		keys = append(keys, mapping.Content[index].Value)
	}
	sort.Strings(keys)
	return keys
}

func sequenceValues(t *testing.T, node *yaml.Node) []string {
	t.Helper()
	if node.Kind == yaml.ScalarNode {
		return []string{node.Value}
	}
	if node.Kind != yaml.SequenceNode {
		t.Fatal("expected YAML scalar or sequence")
	}
	values := make([]string, 0, len(node.Content))
	for _, item := range node.Content {
		if item.Kind != yaml.ScalarNode {
			t.Fatal("expected scalar YAML sequence item")
		}
		values = append(values, item.Value)
	}
	return values
}

func requireSequenceContains(t *testing.T, node *yaml.Node, want, label string) {
	t.Helper()
	for _, value := range sequenceValues(t, node) {
		if value == want {
			return
		}
	}
	t.Fatalf("%s does not contain %q", label, want)
}

func stringSetDifference(want, got []string) []string {
	gotSet := make(map[string]struct{}, len(got))
	for _, value := range got {
		gotSet[value] = struct{}{}
	}
	var missing []string
	for _, value := range want {
		if _, ok := gotSet[value]; !ok {
			missing = append(missing, value)
		}
	}
	return missing
}

func nodeContainsScalar(node *yaml.Node, needle string) bool {
	if node.Kind == yaml.ScalarNode && strings.Contains(node.Value, needle) {
		return true
	}
	for _, child := range node.Content {
		if nodeContainsScalar(child, needle) {
			return true
		}
	}
	return false
}
