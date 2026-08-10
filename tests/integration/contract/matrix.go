package integrationcontract

// The Spark compatibility matrix is derived from the exact connector source
// commit rather than a moving branch. Every row is a concrete test contract,
// not a claim that adjacent axis combinations behave the same way.
//
// Official source:
//   - https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

const sparkConnector0442Commit = "719817782a214b8ca72be520870013a3e0253d92"

var (
	capabilityIDPattern = regexp.MustCompile(`^SBQ-[A-Z0-9]+(?:-[A-Z0-9]+)*-V[1-9][0-9]*$`)
	sha256Pattern       = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

type MatrixState string

const (
	MatrixVerified  MatrixState = "verified"
	MatrixPartial   MatrixState = "partial"
	MatrixGap       MatrixState = "gap"
	MatrixCloudOnly MatrixState = "cloud-only"
)

type MatrixSource struct {
	Kind        string `json:"kind"`
	URL         string `json:"url"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

type MatrixIssue struct {
	URL       string   `json:"url"`
	Languages []string `json:"languages"`
}

type MatrixEvidence struct {
	Kind        string `json:"kind"`
	Ref         string `json:"ref"`
	Fingerprint string `json:"fingerprint"`
}

// MatrixAxes intentionally uses scalar fields. An array would make it unclear
// which cross-product was tested and could conceal an unsupported combination.
type MatrixAxes struct {
	Operation         string `json:"operation"`
	Transport         string `json:"transport"`
	Execution         string `json:"execution"`
	Format            string `json:"format"`
	Delivery          string `json:"delivery"`
	SaveMode          string `json:"saveMode"`
	ReadShape         string `json:"readShape"`
	TablePartitioning string `json:"tablePartitioning"`
	Parallelism       string `json:"parallelism"`
	TypeFamily        string `json:"typeFamily"`
	Auth              string `json:"auth"`
}

type MatrixEntry struct {
	ID                 string           `json:"id"`
	Flow               string           `json:"flow"`
	Axes               MatrixAxes       `json:"axes"`
	State              MatrixState      `json:"state"`
	OfficialSourceRefs []string         `json:"officialSourceRefs"`
	Evidence           []MatrixEvidence `json:"evidence,omitempty"`
	IssueRef           string           `json:"issueRef,omitempty"`
	Limitation         string           `json:"limitation,omitempty"`
}

type CapabilityMatrix struct {
	SchemaVersion   string                  `json:"schemaVersion"`
	ID              string                  `json:"id"`
	ArtifactVariant string                  `json:"artifactVariant"`
	Connector       ConsumerSpec            `json:"connector"`
	SparkVersion    string                  `json:"sparkVersion"`
	SourceCommit    string                  `json:"sourceCommit"`
	Sources         map[string]MatrixSource `json:"sourceCatalog"`
	Issues          map[string]MatrixIssue  `json:"issueCatalog"`
	Entries         []MatrixEntry           `json:"entries"`
}

//go:embed matrices/*.json
var matrixAssets embed.FS

func SparkCapabilityMatrices() ([]CapabilityMatrix, error) {
	paths, err := fs.Glob(matrixAssets, "matrices/*.json")
	if err != nil {
		return nil, err
	}
	matrices := make([]CapabilityMatrix, 0, len(paths))
	for _, path := range paths {
		contents, err := matrixAssets.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var matrix CapabilityMatrix
		decoder := json.NewDecoder(strings.NewReader(string(contents)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&matrix); err != nil {
			return nil, fmt.Errorf("decode capability matrix %s: %w", path, err)
		}
		if err := validateCapabilityMatrix(path, matrix); err != nil {
			return nil, err
		}
		matrices = append(matrices, matrix)
	}
	sort.Slice(matrices, func(i, j int) bool { return matrices[i].ID < matrices[j].ID })
	return matrices, nil
}

func validateCapabilityMatrix(path string, matrix CapabilityMatrix) error {
	if matrix.SchemaVersion != "1" {
		return fmt.Errorf("%s: unsupported schemaVersion %q", path, matrix.SchemaVersion)
	}
	variantConsumers := map[string]string{
		"dsv1-with-dependencies-2.12": "spark-bigquery-with-dependencies_2.12",
		"dsv2-spark-3.5-raw":          "spark-3.5-bigquery-raw",
	}
	if matrix.ID == "" || matrix.Connector.Kind != "connector-artifact" ||
		variantConsumers[matrix.ArtifactVariant] != matrix.Connector.Name || len(matrix.Connector.Versions) != 1 {
		return fmt.Errorf("%s: matrix must bind one exact Spark connector consumer", path)
	}
	version := matrix.Connector.Versions[0]
	if version != "0.44.2" || matrix.SourceCommit != sparkConnector0442Commit || matrix.SparkVersion != "3.5.8" {
		return fmt.Errorf("%s: unreviewed version binding connector=%q spark=%q commit=%q", path, version, matrix.SparkVersion, matrix.SourceCommit)
	}
	if len(matrix.Entries) == 0 {
		return fmt.Errorf("%s: matrix has no entries", path)
	}
	for id, source := range matrix.Sources {
		if id == "" || !isImmutableOfficialSource(source, version, matrix.SourceCommit) {
			return fmt.Errorf("%s: source %q is mutable, unofficial, or unpinned: %q", path, id, source.URL)
		}
	}
	for id, issue := range matrix.Issues {
		if id == "" || !strings.HasPrefix(issue.URL, "https://github.com/leeyh0216/go-bemu/issues/") ||
			len(issue.Languages) != 2 || issue.Languages[0] != "en" || issue.Languages[1] != "ko" {
			return fmt.Errorf("%s: issue %q must be a bilingual go-bemu issue reference", path, id)
		}
	}

	seenIDs := make(map[string]struct{}, len(matrix.Entries))
	seenAxes := make(map[string]string, len(matrix.Entries))
	for index, entry := range matrix.Entries {
		location := fmt.Sprintf("%s: entries[%d]", path, index)
		if !capabilityIDPattern.MatchString(entry.ID) {
			return fmt.Errorf("%s has unstable capability id %q", location, entry.ID)
		}
		if _, exists := seenIDs[entry.ID]; exists {
			return fmt.Errorf("%s duplicates capability id %q", location, entry.ID)
		}
		seenIDs[entry.ID] = struct{}{}
		if err := validateMatrixEntry(location, matrix, entry); err != nil {
			return err
		}
		encodedAxes, _ := json.Marshal(struct {
			Flow string     `json:"flow"`
			Axes MatrixAxes `json:"axes"`
		}{entry.Flow, entry.Axes})
		key := string(encodedAxes)
		if previous := seenAxes[key]; previous != "" {
			return fmt.Errorf("%s duplicates exact axes from %s", location, previous)
		}
		seenAxes[key] = entry.ID
	}
	return nil
}

func validateMatrixEntry(location string, matrix CapabilityMatrix, entry MatrixEntry) error {
	allowedStates := map[MatrixState]bool{
		MatrixVerified: true, MatrixPartial: true, MatrixGap: true, MatrixCloudOnly: true,
	}
	if !allowedStates[entry.State] {
		return fmt.Errorf("%s %s is unclassified: state=%q", location, entry.ID, entry.State)
	}
	allowedFlows := map[string]bool{
		"read-storage": true, "write-direct-pending": true, "write-direct-default": true,
		"write-direct-overwrite": true, "write-indirect-load": true,
		"write-structured-streaming": true, "authentication": true,
		"artifact-bootstrap": true,
	}
	if !allowedFlows[entry.Flow] {
		return fmt.Errorf("%s %s has unknown flow %q", location, entry.ID, entry.Flow)
	}
	if err := validateAxes(location, entry.Axes); err != nil {
		return err
	}
	if len(entry.OfficialSourceRefs) == 0 {
		return fmt.Errorf("%s %s has no official exact-version source", location, entry.ID)
	}
	for _, ref := range entry.OfficialSourceRefs {
		source, ok := matrix.Sources[ref]
		if !ok || !isImmutableConnectorSource(source.URL, matrix.SourceCommit) {
			return fmt.Errorf("%s %s source ref %q is missing or is not exact connector source", location, entry.ID, ref)
		}
	}
	evidenceKinds := make(map[string]bool, len(entry.Evidence))
	for _, evidence := range entry.Evidence {
		if evidence.Kind == "" || evidence.Ref == "" || !sha256Pattern.MatchString(evidence.Fingerprint) {
			return fmt.Errorf("%s %s has incomplete evidence", location, entry.ID)
		}
		if evidenceKinds[evidence.Kind] {
			return fmt.Errorf("%s %s duplicates evidence kind %q", location, entry.ID, evidence.Kind)
		}
		evidenceKinds[evidence.Kind] = true
	}
	if entry.State == MatrixVerified {
		if !evidenceKinds["real-process-trace"] || !evidenceKinds["artifact-lock"] {
			return fmt.Errorf("%s %s claims verified without process trace and artifact lock", location, entry.ID)
		}
	} else {
		if entry.Limitation == "" {
			return fmt.Errorf("%s %s must state its current limitation", location, entry.ID)
		}
		if _, ok := matrix.Issues[entry.IssueRef]; !ok {
			return fmt.Errorf("%s %s must reference a bilingual tracking issue", location, entry.ID)
		}
	}
	return nil
}

func validateAxes(location string, axes MatrixAxes) error {
	allowed := map[string]map[string]bool{
		"operation":         stringSet("read", "write", "auth", "bootstrap"),
		"transport":         stringSet("rest+storage-read", "rest+storage-write", "rest+gcs+load", "credential-provider", "local-classpath"),
		"execution":         stringSet("batch", "structured-streaming", "not-applicable"),
		"format":            stringSet("ARROW", "AVRO", "PROTO_ROWS", "PARQUET", "ORC", "not-applicable"),
		"delivery":          stringSet("exactly-once", "at-least-once", "not-applicable"),
		"saveMode":          stringSet("append", "overwrite", "ignore", "error-if-exists", "not-applicable"),
		"readShape":         stringSet("table", "projection", "filter", "count", "query", "view", "not-applicable"),
		"tablePartitioning": stringSet("unpartitioned", "time", "ingestion-time", "integer-range", "dynamic-overwrite", "not-applicable"),
		"parallelism":       stringSet("read-stream-1", "read-stream-2", "read-stream-4", "read-stream-16", "read-stream-negotiated", "spark-partition-1", "spark-partition-2", "spark-partition-4", "spark-partition-16", "spark-partition-negotiated", "not-applicable"),
		"typeFamily":        stringSet("boolean-integer-float", "string-bytes", "numeric-bignumeric", "temporal", "struct-array", "json", "map", "ml-vector-matrix", "geography", "not-applicable"),
		"auth":              stringSet("static-access-token", "service-account-file", "service-account-base64", "adc-service-account", "adc-user", "wif-external-account", "impersonation", "custom-token-provider", "not-applicable"),
	}
	values := map[string]string{
		"operation": axes.Operation, "transport": axes.Transport, "execution": axes.Execution,
		"format": axes.Format, "delivery": axes.Delivery, "saveMode": axes.SaveMode,
		"readShape": axes.ReadShape, "tablePartitioning": axes.TablePartitioning,
		"parallelism": axes.Parallelism,
		"typeFamily":  axes.TypeFamily, "auth": axes.Auth,
	}
	for name, value := range values {
		if !allowed[name][value] {
			return fmt.Errorf("%s has unknown %s axis %q", location, name, value)
		}
	}
	return nil
}

func stringSet(values ...string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func isImmutableOfficialSource(source MatrixSource, version, commit string) bool {
	parsed, err := url.Parse(source.URL)
	if err != nil || parsed.Scheme != "https" {
		return false
	}
	if isImmutableConnectorSource(source.URL, commit) {
		return true
	}
	if parsed.Host == "repo.maven.apache.org" && strings.Contains(parsed.Path, "/com/google/cloud/spark/") &&
		strings.Contains(parsed.Path, "/"+version+"/") && strings.Contains(parsed.Path, "-"+version) {
		return sha256Pattern.MatchString(source.Fingerprint)
	}
	return false
}

func isImmutableConnectorSource(rawURL, commit string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" {
		return false
	}
	prefix := "/GoogleCloudDataproc/spark-bigquery-connector/blob/" + commit + "/"
	// A source file legitimately contains /src/main/. Mutability is decided by
	// the ref segment immediately after /blob/, which the full-SHA prefix pins.
	return strings.HasPrefix(parsed.Path, prefix)
}
