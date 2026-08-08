package duckdb

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

var queryDecimalTypePattern = regexp.MustCompile(`(?i)^DECIMAL\(([0-9]+),\s*([0-9]+)\)$`)

func bigQueryResultField(name, databaseType string, hint *domain.Field) (domain.Field, error) {
	field, err := parseDuckDBResultTypeWithHint(strings.TrimSpace(databaseType), hint)
	if err != nil {
		return domain.Field{}, fmt.Errorf("map DuckDB query column %q type %q: %w", name, databaseType, err)
	}
	field.Name = name
	return field, nil
}

func parseDuckDBResultType(input string) (domain.Field, error) {
	return parseDuckDBResultTypeWithHint(input, nil)
}

func parseDuckDBResultTypeWithHint(input string, hint *domain.Field) (domain.Field, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return domain.Field{}, fmt.Errorf("empty result type")
	}
	repeated := false
	if strings.HasSuffix(input, "[]") {
		repeated = true
		input = strings.TrimSpace(strings.TrimSuffix(input, "[]"))
	} else if strings.HasPrefix(strings.ToUpper(input), "LIST(") && strings.HasSuffix(input, ")") {
		repeated = true
		input = strings.TrimSpace(input[len("LIST(") : len(input)-1])
	}
	if strings.HasSuffix(input, "[]") || strings.HasPrefix(strings.ToUpper(input), "LIST(") {
		return domain.Field{}, fmt.Errorf("nested arrays are not a BigQuery schema type")
	}

	mode := "NULLABLE"
	if repeated {
		mode = "REPEATED"
	}
	upper := strings.ToUpper(input)
	if strings.HasPrefix(upper, "STRUCT(") && strings.HasSuffix(input, ")") {
		body := input[len("STRUCT(") : len(input)-1]
		parts, err := splitDuckDBTypeList(body)
		if err != nil {
			return domain.Field{}, err
		}
		children := make([]domain.Field, len(parts))
		for index, part := range parts {
			childName, childType, err := splitDuckDBStructField(part)
			if err != nil {
				return domain.Field{}, err
			}
			child, err := parseDuckDBResultTypeWithHint(childType, querySchemaHintByName(hint, childName))
			if err != nil {
				return domain.Field{}, fmt.Errorf("STRUCT field %q: %w", childName, err)
			}
			child.Name = childName
			children[index] = child
		}
		return domain.Field{Type: "RECORD", Mode: mode, Fields: children}, nil
	}
	if match := queryDecimalTypePattern.FindStringSubmatch(input); match != nil {
		precision, _ := strconv.ParseInt(match[1], 10, 64)
		scale, _ := strconv.ParseInt(match[2], 10, 64)
		fieldType := ""
		if scale > 9 || precision-scale > 29 {
			fieldType = "BIGNUMERIC"
		} else if hint != nil && (strings.EqualFold(hint.Type, "NUMERIC") || strings.EqualFold(hint.Type, "BIGNUMERIC")) {
			parameters, err := hint.EffectiveDecimalParameters()
			if err != nil {
				return domain.Field{}, err
			}
			if parameters.Precision != precision || parameters.Scale != scale {
				return domain.Field{}, fmt.Errorf(
					"%w: decimal result shape differs from canonical destination field %q",
					domain.ErrPrecondition, hint.Name,
				)
			}
			fieldType = strings.ToUpper(hint.Type)
		} else {
			return domain.Field{}, fmt.Errorf(
				"%w: capability=%s DECIMAL(%d,%d) does not identify NUMERIC versus BIGNUMERIC",
				domain.ErrUnsupported, domain.GapQueryDecimalLineageV1, precision, scale,
			)
		}
		field := domain.Field{Type: fieldType, Mode: mode, Precision: &precision, Scale: &scale}
		if _, err := field.EffectiveDecimalParameters(); err != nil {
			return domain.Field{}, err
		}
		return field, nil
	}

	fieldType := bigQueryType(input)
	if fieldType == "ARRAY" || fieldType == "RECORD" || strings.HasPrefix(fieldType, "UNSUPPORTED:") {
		return domain.Field{}, fmt.Errorf("unsupported result type %q", input)
	}
	return domain.Field{Type: fieldType, Mode: mode}, nil
}

func querySchemaHintByName(parent *domain.Field, name string) *domain.Field {
	if parent == nil {
		return nil
	}
	for index := range parent.Fields {
		if strings.EqualFold(parent.Fields[index].Name, name) {
			return &parent.Fields[index]
		}
	}
	return nil
}

func splitDuckDBTypeList(input string) ([]string, error) {
	var result []string
	start, depth := 0, 0
	inQuote := false
	for index := 0; index < len(input); index++ {
		switch input[index] {
		case '"':
			if inQuote && index+1 < len(input) && input[index+1] == '"' {
				index++
				continue
			}
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote {
				depth--
				if depth < 0 {
					return nil, fmt.Errorf("unbalanced result type")
				}
			}
		case ',':
			if !inQuote && depth == 0 {
				result = append(result, strings.TrimSpace(input[start:index]))
				start = index + 1
			}
		}
	}
	if inQuote || depth != 0 {
		return nil, fmt.Errorf("unbalanced result type")
	}
	last := strings.TrimSpace(input[start:])
	if last == "" {
		return nil, fmt.Errorf("empty STRUCT field")
	}
	return append(result, last), nil
}

func splitDuckDBStructField(input string) (string, string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", fmt.Errorf("empty STRUCT field")
	}
	if input[0] == '"' {
		var name strings.Builder
		for index := 1; index < len(input); index++ {
			if input[index] != '"' {
				name.WriteByte(input[index])
				continue
			}
			if index+1 < len(input) && input[index+1] == '"' {
				name.WriteByte('"')
				index++
				continue
			}
			typeName := strings.TrimSpace(input[index+1:])
			if typeName == "" {
				return "", "", fmt.Errorf("STRUCT field %q has no type", name.String())
			}
			return name.String(), typeName, nil
		}
		return "", "", fmt.Errorf("unterminated STRUCT field name")
	}
	for index, value := range input {
		if value == ' ' || value == '\t' {
			name, typeName := input[:index], strings.TrimSpace(input[index:])
			if name == "" || typeName == "" {
				break
			}
			return name, typeName, nil
		}
	}
	return "", "", fmt.Errorf("STRUCT field %q has no type", input)
}

func queryResultSchema(columnTypes []*sql.ColumnType, hints []domain.Field) ([]domain.Field, error) {
	fields := make([]domain.Field, len(columnTypes))
	for index, columnType := range columnTypes {
		var hint *domain.Field
		if index < len(hints) {
			hint = &hints[index]
		}
		field, err := bigQueryResultField(columnType.Name(), columnType.DatabaseTypeName(), hint)
		if err != nil {
			return nil, err
		}
		fields[index] = field
	}
	return fields, nil
}
