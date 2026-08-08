package static

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
)

type tokenSetDocument struct {
	APIVersion string       `yaml:"apiVersion"`
	Kind       string       `yaml:"kind"`
	Tokens     []tokenEntry `yaml:"tokens"`
}

// UnmarshalYAML rejects non-mapping roots, custom tags, coercions, duplicate
// keys, aliases in schema fields, and unknown fields before credential values
// enter the domain model.
//   - YAML representation nodes: https://yaml.org/spec/1.2.2/#3211-nodes
//   - YAML tags: https://yaml.org/spec/1.2.2/#10212-tag-property
func (document *tokenSetDocument) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content)%2 != 0 {
		return errors.New("token set document must be a standard mapping")
	}
	seen := make(map[string]struct{}, 3)
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index]
		value := node.Content[index+1]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return errors.New("token set field key must be a string")
		}
		if _, duplicate := seen[key.Value]; duplicate {
			return errors.New("duplicate token set field")
		}
		seen[key.Value] = struct{}{}
		switch key.Value {
		case "apiVersion":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
				return errors.New("apiVersion must be an explicit string")
			}
			document.APIVersion = value.Value
		case "kind":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
				return errors.New("kind must be an explicit string")
			}
			document.Kind = value.Value
		case "tokens":
			if value.Kind != yaml.SequenceNode || value.Tag != "!!seq" {
				return errors.New("tokens must be a standard sequence")
			}
			if err := value.Decode(&document.Tokens); err != nil {
				return errors.New("tokens sequence decode failed")
			}
		default:
			return errors.New("unknown token set field")
		}
	}
	return nil
}

type tokenEntry struct {
	Principal string `yaml:"principal"`
	Token     string `yaml:"token"`
}

// UnmarshalYAML prevents yaml.v3's convenient scalar coercions from turning a
// number, boolean, null, or alias into credential text. Credential fields are
// an exact, explicit string contract and duplicate keys are rejected.
func (entry *tokenEntry) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content)%2 != 0 {
		return errors.New("token entry must be a standard mapping")
	}
	seen := make(map[string]struct{}, 2)
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index]
		value := node.Content[index+1]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return errors.New("token entry key must be a string")
		}
		if _, duplicate := seen[key.Value]; duplicate {
			return errors.New("duplicate token entry field")
		}
		seen[key.Value] = struct{}{}
		if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
			return errors.New("token entry field must be an explicit string")
		}
		switch key.Value {
		case "principal":
			entry.Principal = value.Value
		case "token":
			entry.Token = value.Value
		default:
			return errors.New("unknown token entry field")
		}
	}
	return nil
}

func decodeRecords(payload []byte, options Options) ([]tokenRecord, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(payload))
	var document tokenSetDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, invalidTokenSet(authdomain.DiagnosticManifestYAMLDecode, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, invalidTokenSet(authdomain.DiagnosticManifestMultipleDocuments, nil)
		}
		return nil, invalidTokenSet(authdomain.DiagnosticManifestTrailingYAML, err)
	}
	if document.APIVersion != ManifestAPIVersion || document.Kind != ManifestKind {
		return nil, invalidTokenSet(authdomain.DiagnosticManifestIdentityMismatch, nil)
	}
	if len(document.Tokens) == 0 {
		return nil, invalidTokenSet(authdomain.DiagnosticManifestEmpty, nil)
	}
	if len(document.Tokens) > options.MaxTokens {
		return nil, invalidTokenSet(authdomain.DiagnosticManifestTokenCount, nil)
	}

	records := make([]tokenRecord, 0, len(document.Tokens))
	seen := make(map[[sha256.Size]byte]struct{}, len(document.Tokens))
	for _, entry := range document.Tokens {
		principalBytes := []byte(entry.Principal)
		if err := validatePrincipal(entry.Principal, options.MaxPrincipalBytes); err != nil {
			clear(principalBytes)
			return nil, err
		}
		principal, err := authdomain.NewPrincipal(authdomain.CredentialStatic, principalBytes)
		clear(principalBytes)
		if err != nil {
			return nil, invalidTokenSet(authdomain.DiagnosticManifestPrincipalBuild, err)
		}

		tokenBytes := []byte(entry.Token)
		if err := authdomain.ValidateBearerToken(tokenBytes, options.MinTokenBytes, options.MaxTokenBytes); err != nil {
			clear(tokenBytes)
			return nil, invalidTokenSet(authdomain.DiagnosticManifestTokenInvalid, err)
		}
		tokenDigest := sha256.Sum256(tokenBytes)
		clear(tokenBytes)
		if _, duplicate := seen[tokenDigest]; duplicate {
			return nil, invalidTokenSet(authdomain.DiagnosticManifestTokenDuplicate, nil)
		}
		seen[tokenDigest] = struct{}{}
		records = append(records, tokenRecord{tokenDigest: tokenDigest, principal: principal})
	}
	return records, nil
}

func validatePrincipal(principal string, maxBytes int) error {
	if principal == "" || len(principal) > maxBytes || !utf8.ValidString(principal) {
		return invalidTokenSet(authdomain.DiagnosticManifestPrincipalSize, nil)
	}
	for _, character := range principal {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return invalidTokenSet(authdomain.DiagnosticManifestPrincipalChars, nil)
		}
	}
	return nil
}

func invalidTokenSet(diagnostic authdomain.DiagnosticCode, cause error) error {
	return authdomain.NewError(
		authdomain.ReasonInvalidTokenSet,
		diagnostic,
		cause,
	)
}
