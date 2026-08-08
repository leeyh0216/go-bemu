package docs_test

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	inlineCodePattern      = regexp.MustCompile("`[^`]*`")
	linkDestinationPattern = regexp.MustCompile(`\]\([^)]+\)`)
	htmlCommentPattern     = regexp.MustCompile(`<!--.*?-->`)
	plainNarrativeEnding   = regexp.MustCompile(`(한다|된다|있다|없다|이다|아니다|둔다|남는다|따른다|보존한다|거부한다|사용한다|관리한다|적용한다|구성한다|소유한다|유지한다|확인한다|실행한다|허용한다|지원한다|기록한다|반환한다|전환한다|정의한다|제공한다|필요하다|가능하다|중요하다|유효하다)\.(\s|$)`)
	translationeseTerms    = []struct {
		pattern *regexp.Regexp
		hint    string
	}{
		{regexp.MustCompile(`(?i)(^|[^a-z])(gate)([^a-z]|$)`), "확인 절차, 판정 기준, 변경 조건 또는 잠금"},
		{regexp.MustCompile(`(?i)(^|[^a-z])(gap)([^a-z]|$)`), "미지원 항목 또는 남은 차이"},
		{regexp.MustCompile(`(?i)(^|[^a-z])(shape)([^a-z]|$)`), "요청 구조, 응답 구조 또는 데이터 구조"},
		{regexp.MustCompile(`(?i)(^|[^a-z])(bounded)([^a-z]|$)`), "상한이 있는 또는 최대 크기가 정해진"},
		{regexp.MustCompile(`(?i)(^|[^a-z])(freeze|slice|runbook|fixture)([^a-z]|$)`), "문맥에 맞는 한국어"},
		{regexp.MustCompile(`(?i)public[ -]edge|fail[ -]closed`), "공개 API 경계 또는 안전하게 거부"},
	}
)

func TestKoreanDocumentationStyle(t *testing.T) {
	root := filepath.Clean("..")
	documents := []string{
		filepath.Join(root, "README.ko.md"),
		filepath.Join(root, "CONTRIBUTING.ko.md"),
	}
	koreanRoot := filepath.Join(root, "docs", "ko")
	if err := filepath.WalkDir(koreanRoot, func(path string, entry os.DirEntry, err error) error {
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

	for _, document := range documents {
		checkKoreanDocumentStyle(t, document)
	}
}

func checkKoreanDocumentStyle(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	inCodeFence := false
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inCodeFence = !inCodeFence
			continue
		}
		if inCodeFence {
			continue
		}

		prose := inlineCodePattern.ReplaceAllString(line, "")
		prose = linkDestinationPattern.ReplaceAllString(prose, "]")
		prose = htmlCommentPattern.ReplaceAllString(prose, "")
		if plainNarrativeEnding.MatchString(prose) {
			t.Errorf("%s:%d uses plain narrative style; use polite explanatory Korean", path, lineNumber)
		}
		for _, term := range translationeseTerms {
			if term.pattern.MatchString(prose) {
				t.Errorf("%s:%d uses translationese; prefer %s", path, lineNumber, term.hint)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}
