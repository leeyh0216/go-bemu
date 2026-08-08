package docs_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var (
	docIDPattern       = regexp.MustCompile(`<!--\s*doc-id:\s*([a-z0-9./_-]+)\s*-->`)
	sectionPattern     = regexp.MustCompile(`<!--\s*section:\s*([a-z0-9_-]+)\s*-->`)
	absoluteURLPattern = regexp.MustCompile(`https://[^)\s>]+`)
	markdownLink       = regexp.MustCompile(`\[[^]]+\]\(([^)]+)\)`)
	mutableSourceLink  = regexp.MustCompile(`github\.com/(GoogleCloudDataproc/(spark-bigquery-connector|flink-bigquery-connector)|goccy/bigquery-emulator)/(blob|tree)/(main|master)(/|\b)`)
	sparkVersionClaim  = regexp.MustCompile(`(?is)(connector.{0,160}0\.44\.2|0\.44\.2.{0,160}connector)`)
	flinkVersionClaim  = regexp.MustCompile(`(?is)(flink.{0,160}1\.2\.0|1\.2\.0.{0,160}flink)`)
	goccyVersionClaim  = regexp.MustCompile(`(?is)(goccy.{0,160}v?0\.8\.1|v?0\.8\.1.{0,160}goccy)`)
	pinnedConnectorURL = regexp.MustCompile(`github\.com/GoogleCloudDataproc/spark-bigquery-connector/(blob|tree)/(0\.44\.2|[0-9a-f]{40})(/|\b)`)
	pinnedFlinkURL     = regexp.MustCompile(`github\.com/GoogleCloudDataproc/flink-bigquery-connector/(blob|tree)/1\.2\.0(/|\b)`)
	pinnedGoccyURL     = regexp.MustCompile(`github\.com/goccy/bigquery-emulator/(blob|tree)/v0\.8\.1(/|\b)`)
	primarySourceURL   = regexp.MustCompile(`https://(cloud\.google\.com/|github\.com/(GoogleCloudDataproc/(spark-bigquery-connector/(blob|tree)/(0\.44\.2|[0-9a-f]{40})|flink-bigquery-connector/(blob|tree)/1\.2\.0)|goccy/bigquery-emulator/(blob|tree)/v0\.8\.1)|duckdb\.org/|arrow\.apache\.org/|avro\.apache\.org/)`)
)

func TestBilingualDocumentationParityAndProvenance(t *testing.T) {
	root := filepath.Clean("..")
	pairs := [][2]string{
		{filepath.Join(root, "README.md"), filepath.Join(root, "README.ko.md")},
		{filepath.Join(root, "CONTRIBUTING.md"), filepath.Join(root, "CONTRIBUTING.ko.md")},
	}

	englishRoot := filepath.Join(root, "docs", "en")
	err := filepath.WalkDir(englishRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		relative, err := filepath.Rel(englishRoot, path)
		if err != nil {
			return err
		}
		pairs = append(pairs, [2]string{path, filepath.Join(root, "docs", "ko", relative)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) < 6 {
		t.Fatalf("documentation parity guard found only %d pairs", len(pairs))
	}

	for _, pair := range pairs {
		pair := pair
		t.Run(filepath.ToSlash(pair[0]), func(t *testing.T) {
			english := readDocument(t, pair[0])
			korean := readDocument(t, pair[1])
			assertLanguageDocument(t, pair[0], english, "en")
			assertLanguageDocument(t, pair[1], korean, "ko")

			if englishID, koreanID := firstMatch(docIDPattern, english), firstMatch(docIDPattern, korean); englishID != koreanID {
				t.Fatalf("doc-id mismatch: English=%q Korean=%q", englishID, koreanID)
			}
			if englishSections, koreanSections := allMatches(sectionPattern, english), allMatches(sectionPattern, korean); !reflect.DeepEqual(englishSections, koreanSections) {
				t.Fatalf("section order mismatch:\nEnglish=%v\nKorean=%v", englishSections, koreanSections)
			}
			if englishSources, koreanSources := sourceURLs(english), sourceURLs(korean); !reflect.DeepEqual(englishSources, koreanSources) {
				t.Fatalf("primary/source URL mismatch:\nEnglish=%v\nKorean=%v", englishSources, koreanSources)
			}
		})
	}

	assertNoOrphanKoreanDocuments(t, root, englishRoot)
}

func TestDocumentationRelativeLinksResolve(t *testing.T) {
	root := filepath.Clean("..")
	var documents []string
	for _, path := range []string{filepath.Join(root, "README.md"), filepath.Join(root, "README.ko.md"), filepath.Join(root, "CONTRIBUTING.md"), filepath.Join(root, "CONTRIBUTING.ko.md")} {
		documents = append(documents, path)
	}
	for _, language := range []string{"en", "ko"} {
		languageRoot := filepath.Join(root, "docs", language)
		if err := filepath.WalkDir(languageRoot, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && filepath.Ext(path) == ".md" {
				documents = append(documents, path)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, document := range documents {
		contents := readDocument(t, document)
		for _, match := range markdownLink.FindAllStringSubmatch(contents, -1) {
			target := strings.SplitN(match[1], "#", 2)[0]
			if target == "" || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			resolved := filepath.Clean(filepath.Join(filepath.Dir(document), filepath.FromSlash(target)))
			if _, err := os.Stat(resolved); err != nil {
				t.Errorf("%s has unresolved relative link %q: %v", document, target, err)
			}
		}
	}
}

func TestIssueTemplatesRequireBilingualBodies(t *testing.T) {
	root := filepath.Clean("..")
	templateRoot := filepath.Join(root, ".github", "ISSUE_TEMPLATE")
	entries, err := os.ReadDir(templateRoot)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".md" {
			continue
		}
		checked++
		path := filepath.Join(templateRoot, entry.Name())
		contents := readDocument(t, path)
		englishIndex := strings.Index(contents, "## English\n")
		koreanIndex := strings.Index(contents, "## 한국어\n")
		if englishIndex < 0 || koreanIndex <= englishIndex {
			t.Errorf("%s must contain ordered English and Korean body sections", path)
		}
		for _, heading := range []string{
			"### Goal", "### Scope", "### Acceptance", "### Exclusions and dependencies", "### Sources",
			"### 목표", "### 범위", "### 수용 기준", "### 제외 범위와 의존성", "### 출처",
		} {
			if !strings.Contains(contents, heading) {
				t.Errorf("%s is missing %q", path, heading)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no Markdown issue templates were checked")
	}
}

func TestVersionProvenancePatternsCoverWrappedClaims(t *testing.T) {
	tests := []struct {
		name   string
		claim  *regexp.Regexp
		pinned *regexp.Regexp
		text   string
		url    string
	}{
		{
			name: "spark", claim: sparkVersionClaim, pinned: pinnedConnectorURL,
			text: "Spark BigQuery connector\n0.44.2",
			url:  "github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2",
		},
		{
			name: "flink", claim: flinkVersionClaim, pinned: pinnedFlinkURL,
			text: "Flink connector\n1.2.0",
			url:  "github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/pom.xml",
		},
		{
			name: "goccy", claim: goccyVersionClaim, pinned: pinnedGoccyURL,
			text: "goccy BigQuery emulator\nv0.8.1",
			url:  "github.com/goccy/bigquery-emulator/tree/v0.8.1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !test.claim.MatchString(test.text) {
				t.Fatalf("version claim pattern missed wrapped text %q", test.text)
			}
			if !test.pinned.MatchString(test.url) {
				t.Fatalf("pinned-source pattern rejected %q", test.url)
			}
		})
	}

	for _, mutable := range []string{
		"github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/master/file.java",
		"github.com/GoogleCloudDataproc/flink-bigquery-connector/tree/main/module",
		"github.com/goccy/bigquery-emulator/tree/master/server",
	} {
		if !mutableSourceLink.MatchString(mutable) {
			t.Errorf("mutable-source pattern missed %q", mutable)
		}
	}
}

func assertLanguageDocument(t *testing.T, path, contents, language string) {
	t.Helper()
	if firstMatch(docIDPattern, contents) == "" {
		t.Fatalf("%s has no doc-id marker", path)
	}
	if !strings.Contains(contents, "<!-- lang: "+language+" -->") {
		t.Fatalf("%s has no %s language marker", path, language)
	}
	if !strings.Contains(contents, "[English](") || !strings.Contains(contents, "[한국어](") {
		t.Fatalf("%s has no bilingual language switch", path)
	}
	if len(sectionPattern.FindAllStringSubmatch(contents, -1)) == 0 {
		t.Fatalf("%s has no section markers", path)
	}
	if !primarySourceURL.MatchString(contents) {
		t.Fatalf("%s has no recognized primary-source URL", path)
	}
	if mutableSourceLink.MatchString(contents) {
		t.Fatalf("%s contains a mutable upstream source link", path)
	}
	if sparkVersionClaim.MatchString(contents) && !pinnedConnectorURL.MatchString(contents) {
		t.Fatalf("%s claims connector 0.44.2 behavior without an immutable tagged or commit source link", path)
	}
	if flinkVersionClaim.MatchString(contents) && !pinnedFlinkURL.MatchString(contents) {
		t.Fatalf("%s claims Flink connector 1.2.0 behavior without an exact tagged source link", path)
	}
	if goccyVersionClaim.MatchString(contents) && !pinnedGoccyURL.MatchString(contents) {
		t.Fatalf("%s claims goccy emulator v0.8.1 behavior without an exact tagged source link", path)
	}
}

func assertNoOrphanKoreanDocuments(t *testing.T, root, englishRoot string) {
	t.Helper()
	koreanRoot := filepath.Join(root, "docs", "ko")
	if err := filepath.WalkDir(koreanRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		relative, err := filepath.Rel(koreanRoot, path)
		if err != nil {
			return err
		}
		if _, err := os.Stat(filepath.Join(englishRoot, relative)); err != nil {
			t.Errorf("Korean document %s has no English counterpart", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func readDocument(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(contents)
}

func firstMatch(pattern *regexp.Regexp, value string) string {
	match := pattern.FindStringSubmatch(value)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func allMatches(pattern *regexp.Regexp, value string) []string {
	matches := pattern.FindAllStringSubmatch(value, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		result = append(result, match[1])
	}
	return result
}

func sourceURLs(value string) []string {
	set := make(map[string]struct{})
	for _, raw := range absoluteURLPattern.FindAllString(value, -1) {
		set[strings.TrimRight(raw, ".,;")] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for source := range set {
		result = append(result, source)
	}
	sort.Strings(result)
	return result
}
