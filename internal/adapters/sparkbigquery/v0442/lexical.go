package v0442

import (
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func scanQuotedLiteral(statement string, start int, quote byte) (int, error) {
	for index := start + 1; index < len(statement); index++ {
		if statement[index] == '\\' {
			index++
			continue
		}
		if statement[index] != quote {
			continue
		}
		if index+1 < len(statement) && statement[index+1] == quote {
			index++
			continue
		}
		return index + 1, nil
	}
	return 0, fmt.Errorf("%w: unterminated quoted SQL literal", domain.ErrInvalid)
}

func scanBacktickIdentifier(statement string, start int) (string, int, error) {
	var identifier strings.Builder
	for index := start + 1; index < len(statement); index++ {
		if statement[index] == '\\' && index+1 < len(statement) {
			index++
			identifier.WriteByte(statement[index])
			continue
		}
		if statement[index] == '`' {
			if identifier.Len() == 0 {
				return "", 0, fmt.Errorf("%w: quoted identifier cannot be empty", domain.ErrInvalid)
			}
			return identifier.String(), index + 1, nil
		}
		identifier.WriteByte(statement[index])
	}
	return "", 0, fmt.Errorf("%w: unterminated backtick identifier", domain.ErrInvalid)
}

func scanLineComment(statement string, start int) int {
	if newline := strings.IndexByte(statement[start:], '\n'); newline >= 0 {
		return start + newline + 1
	}
	return len(statement)
}

func scanBlockComment(statement string, start int) (int, error) {
	if end := strings.Index(statement[start+2:], "*/"); end >= 0 {
		return start + 2 + end + 2, nil
	}
	return 0, fmt.Errorf("%w: unterminated block comment", domain.ErrInvalid)
}

func isIdentifierStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func isIdentifierPart(value byte) bool {
	return isIdentifierStart(value) || value >= '0' && value <= '9'
}

func isSQLSpace(value byte) bool {
	switch value {
	case ' ', '\t', '\r', '\n', '\f':
		return true
	default:
		return false
	}
}

func leadingStatementKeyword(statement string) string {
	for index := 0; index < len(statement); {
		switch {
		case isSQLSpace(statement[index]):
			index++
		case statement[index] == '#' || statement[index] == '-' && index+1 < len(statement) && statement[index+1] == '-':
			index = scanLineComment(statement, index)
		case statement[index] == '/' && index+1 < len(statement) && statement[index+1] == '*':
			end, err := scanBlockComment(statement, index)
			if err != nil {
				return "UNKNOWN"
			}
			index = end
		case isIdentifierStart(statement[index]):
			end := index + 1
			for end < len(statement) && isIdentifierPart(statement[end]) {
				end++
			}
			return strings.ToUpper(statement[index:end])
		default:
			return "UNKNOWN"
		}
	}
	return "UNKNOWN"
}

func connectorTableReference(request ports.QueryRequest, identifier string) (domain.TableReference, error) {
	parts := strings.Split(identifier, ".")
	for _, part := range parts {
		if part == "" {
			return domain.TableReference{}, fmt.Errorf("%w: malformed quoted table reference", domain.ErrInvalid)
		}
	}
	switch len(parts) {
	case 3:
		return domain.TableReference{ProjectID: parts[0], DatasetID: parts[1], TableID: parts[2]}, nil
	case 2:
		if request.ProjectID == "" {
			return domain.TableReference{}, fmt.Errorf("%w: project is required for two-part table reference", domain.ErrInvalid)
		}
		return domain.TableReference{ProjectID: request.ProjectID, DatasetID: parts[0], TableID: parts[1]}, nil
	default:
		return domain.TableReference{}, fmt.Errorf("%w: connector table reference must contain two or three parts", domain.ErrInvalid)
	}
}
