package domain

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// QueryParameter is the immutable, wire-independent value supplied to a
// GoogleSQL query. Value is kept in its canonical textual form so precision is
// never lost while a driver-specific adapter performs the final bind lowering.
type QueryParameter struct {
	Name  string
	Type  string
	Value string
}

type QueryParameterMode string

const (
	QueryParameterNamed      QueryParameterMode = "NAMED"
	QueryParameterPositional QueryParameterMode = "POSITIONAL"
)

func normalizeQueryParameters(mode QueryParameterMode, parameters []QueryParameter) (QueryParameterMode, []QueryParameter, error) {
	if len(parameters) == 0 {
		if mode != "" {
			return "", nil, fmt.Errorf("%w: parameterMode requires queryParameters", ErrInvalid)
		}
		return "", nil, nil
	}
	mode = QueryParameterMode(strings.ToUpper(string(mode)))
	if mode != QueryParameterNamed && mode != QueryParameterPositional {
		return "", nil, fmt.Errorf("%w: parameterMode must be NAMED or POSITIONAL", ErrInvalid)
	}
	copy := append([]QueryParameter(nil), parameters...)
	seen := map[string]struct{}{}
	for i := range copy {
		copy[i].Type = strings.ToUpper(copy[i].Type)
		if !supportedQueryParameterType(copy[i].Type) {
			return "", nil, fmt.Errorf("%w: unsupported query parameter type", ErrInvalid)
		}
		if err := validateQueryParameterValue(copy[i]); err != nil {
			return "", nil, err
		}
		if mode == QueryParameterNamed {
			if !jobIDPattern.MatchString(copy[i].Name) {
				return "", nil, fmt.Errorf("%w: named query parameter requires a valid name", ErrInvalid)
			}
			key := strings.ToLower(copy[i].Name)
			if _, ok := seen[key]; ok {
				return "", nil, fmt.Errorf("%w: duplicate named query parameter", ErrInvalid)
			}
			seen[key] = struct{}{}
		} else if copy[i].Name != "" {
			return "", nil, fmt.Errorf("%w: positional query parameter must not have a name", ErrInvalid)
		}
	}
	return mode, copy, nil
}

func validateQueryParameterValue(parameter QueryParameter) error {
	var err error
	switch parameter.Type {
	case "BOOL":
		_, err = strconv.ParseBool(parameter.Value)
	case "INT64":
		_, err = strconv.ParseInt(parameter.Value, 10, 64)
	case "FLOAT64":
		_, err = strconv.ParseFloat(parameter.Value, 64)
	case "DATE":
		_, err = time.Parse("2006-01-02", parameter.Value)
	case "DATETIME":
		_, err = time.Parse("2006-01-02 15:04:05", parameter.Value)
	case "TIME":
		_, err = time.Parse("15:04:05", parameter.Value)
	case "TIMESTAMP":
		_, err = time.Parse(time.RFC3339Nano, parameter.Value)
	case "JSON":
		if !json.Valid([]byte(parameter.Value)) {
			err = fmt.Errorf("invalid JSON")
		}
	case "NUMERIC":
		if !numericText(parameter.Value) {
			err = fmt.Errorf("invalid NUMERIC")
		}
	}
	if err != nil {
		return fmt.Errorf("%w: invalid %s query parameter value", ErrInvalid, parameter.Type)
	}
	return nil
}

func numericText(value string) bool {
	if value == "" {
		return false
	}
	if value[0] == '+' || value[0] == '-' {
		value = value[1:]
	}
	if value == "" {
		return false
	}
	dot, digits := false, 0
	for _, r := range value {
		if r == '.' && !dot {
			dot = true
			continue
		}
		if r < '0' || r > '9' {
			return false
		}
		digits++
	}
	return digits > 0
}

func supportedQueryParameterType(value string) bool {
	switch value {
	case "BOOL", "INT64", "FLOAT64", "NUMERIC", "STRING", "DATE", "DATETIME", "JSON", "TIME", "TIMESTAMP":
		return true
	}
	return false
}
