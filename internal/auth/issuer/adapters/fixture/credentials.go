package fixture

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/url"
	"unicode"
	"unicode/utf8"

	issuerdomain "github.com/leeyh0216/go-bemu/internal/auth/issuer/domain"
	issuerports "github.com/leeyh0216/go-bemu/internal/auth/issuer/ports"
)

const (
	defaultMaxRegistrations   = 1_000
	defaultMaxCredentialBytes = 64 * 1024
	defaultMaxIdentityBytes   = 4 * 1024
	defaultMaxTokenTypeBytes  = 2 * 1024
	defaultMaxTargetBytes     = 8 * 1024
	defaultMaxScopes          = 128
	defaultMaxScopeBytes      = 2 * 1024
)

type CredentialOptions struct {
	MaxRegistrations   int
	MaxCredentialBytes int
	MaxIdentityBytes   int
	MaxTokenTypeBytes  int
	MaxTargetBytes     int
	MaxScopes          int
	MaxScopeBytes      int
}

func DefaultCredentialOptions() CredentialOptions {
	return CredentialOptions{
		MaxRegistrations:   defaultMaxRegistrations,
		MaxCredentialBytes: defaultMaxCredentialBytes,
		MaxIdentityBytes:   defaultMaxIdentityBytes,
		MaxTokenTypeBytes:  defaultMaxTokenTypeBytes,
		MaxTargetBytes:     defaultMaxTargetBytes,
		MaxScopes:          defaultMaxScopes,
		MaxScopeBytes:      defaultMaxScopeBytes,
	}
}

type RefreshRegistration struct {
	ClientID     string
	ClientSecret string
	RefreshToken string
	Identity     string
	Scopes       []string
}

type SubjectRegistration struct {
	TokenType      string
	Token          string
	Audience       string
	Resource       string
	ActorTokenType string
	ActorToken     string
	Identity       string
	Scopes         []string
}

type refreshRecord struct {
	clientID     [sha256.Size]byte
	clientSecret [sha256.Size]byte
	refreshToken [sha256.Size]byte
	subject      issuerdomain.Subject
}

type subjectRecord struct {
	tokenType      [sha256.Size]byte
	token          [sha256.Size]byte
	audience       [sha256.Size]byte
	resource       [sha256.Size]byte
	actorTokenType [sha256.Size]byte
	actorToken     [sha256.Size]byte
	hasActor       bool
	subject        issuerdomain.Subject
}

type CredentialVerifier struct {
	options  CredentialOptions
	refresh  []refreshRecord
	subjects []subjectRecord
}

func NewCredentialVerifier(refresh []RefreshRegistration, subjects []SubjectRegistration, options CredentialOptions) (*CredentialVerifier, error) {
	if err := validateCredentialOptions(options); err != nil {
		return nil, err
	}
	if len(refresh)+len(subjects) == 0 || len(refresh)+len(subjects) > options.MaxRegistrations {
		return nil, fixtureConfigError()
	}

	verifier := &CredentialVerifier{options: options}
	refreshSeen := make(map[[sha256.Size * 3]byte]struct{}, len(refresh))
	for _, registration := range refresh {
		record, key, err := buildRefreshRecord(registration, options)
		if err != nil {
			return nil, err
		}
		if _, duplicate := refreshSeen[key]; duplicate {
			return nil, fixtureConfigError()
		}
		refreshSeen[key] = struct{}{}
		verifier.refresh = append(verifier.refresh, record)
	}

	subjectSeen := make(map[[sha256.Size * 6]byte]struct{}, len(subjects))
	for _, registration := range subjects {
		record, key, err := buildSubjectRecord(registration, options)
		if err != nil {
			return nil, err
		}
		if _, duplicate := subjectSeen[key]; duplicate {
			return nil, fixtureConfigError()
		}
		subjectSeen[key] = struct{}{}
		verifier.subjects = append(verifier.subjects, record)
	}
	return verifier, nil
}

func (v *CredentialVerifier) VerifyRefresh(ctx context.Context, credential issuerports.RefreshCredential) (issuerdomain.Subject, error) {
	if err := activeContext(ctx); err != nil {
		return issuerdomain.Subject{}, err
	}
	if !boundedCredential(credential.ClientID, v.options.MaxCredentialBytes) ||
		!boundedCredential(credential.ClientSecret, v.options.MaxCredentialBytes) ||
		!boundedCredential(credential.RefreshToken, v.options.MaxCredentialBytes) {
		return issuerdomain.Subject{}, refreshRejected()
	}

	clientID := sha256.Sum256(credential.ClientID)
	clientSecret := sha256.Sum256(credential.ClientSecret)
	refreshToken := sha256.Sum256(credential.RefreshToken)
	matched := 0
	matchedIndex := 0
	for index := range v.refresh {
		equal := subtle.ConstantTimeCompare(clientID[:], v.refresh[index].clientID[:]) &
			subtle.ConstantTimeCompare(clientSecret[:], v.refresh[index].clientSecret[:]) &
			subtle.ConstantTimeCompare(refreshToken[:], v.refresh[index].refreshToken[:])
		matchedIndex = subtle.ConstantTimeSelect(equal, index, matchedIndex)
		matched = subtle.ConstantTimeSelect(equal, 1, matched)
	}
	if err := activeContext(ctx); err != nil {
		return issuerdomain.Subject{}, err
	}
	if matched != 1 {
		return issuerdomain.Subject{}, refreshRejected()
	}
	return v.refresh[matchedIndex].subject.Clone(), nil
}

func (v *CredentialVerifier) VerifySubjectToken(ctx context.Context, credential issuerports.SubjectTokenCredential) (issuerdomain.Subject, error) {
	if err := activeContext(ctx); err != nil {
		return issuerdomain.Subject{}, err
	}
	if !validTokenType(credential.TokenType, v.options.MaxTokenTypeBytes) ||
		!boundedCredential(credential.Token, v.options.MaxCredentialBytes) ||
		!boundedText(credential.Audience, v.options.MaxTargetBytes, true) ||
		!boundedText(credential.Resource, v.options.MaxTargetBytes, true) {
		return issuerdomain.Subject{}, subjectRejected(issuerdomain.DiagnosticSTSSubjectRejected)
	}
	if credential.Actor != nil && (!validTokenType(credential.Actor.TokenType, v.options.MaxTokenTypeBytes) ||
		!boundedCredential(credential.Actor.Token, v.options.MaxCredentialBytes)) {
		return issuerdomain.Subject{}, subjectRejected(issuerdomain.DiagnosticSTSActorRejected)
	}
	if len(credential.Options) > v.options.MaxCredentialBytes {
		return issuerdomain.Subject{}, subjectRejected(issuerdomain.DiagnosticSTSTargetRejected)
	}

	tokenType := sha256.Sum256([]byte(credential.TokenType))
	token := sha256.Sum256(credential.Token)
	audience := sha256.Sum256([]byte(credential.Audience))
	resource := sha256.Sum256([]byte(credential.Resource))
	actorTokenType := sha256.Sum256(nil)
	actorToken := sha256.Sum256(nil)
	hasActor := credential.Actor != nil
	if credential.Actor != nil {
		actorTokenType = sha256.Sum256([]byte(credential.Actor.TokenType))
		actorToken = sha256.Sum256(credential.Actor.Token)
	}

	credentialMatched := 0
	targetMatched := 0
	actorMatched := 0
	matchedIndex := 0
	for index := range v.subjects {
		baseEqual := subtle.ConstantTimeCompare(tokenType[:], v.subjects[index].tokenType[:]) &
			subtle.ConstantTimeCompare(token[:], v.subjects[index].token[:])
		targetEqual := baseEqual &
			subtle.ConstantTimeCompare(audience[:], v.subjects[index].audience[:]) &
			subtle.ConstantTimeCompare(resource[:], v.subjects[index].resource[:])
		hasActorEqual := subtle.ConstantTimeByteEq(boolByte(hasActor), boolByte(v.subjects[index].hasActor))
		actorEqual := targetEqual & hasActorEqual &
			subtle.ConstantTimeCompare(actorTokenType[:], v.subjects[index].actorTokenType[:]) &
			subtle.ConstantTimeCompare(actorToken[:], v.subjects[index].actorToken[:])
		credentialMatched = subtle.ConstantTimeSelect(baseEqual, 1, credentialMatched)
		targetMatched = subtle.ConstantTimeSelect(targetEqual, 1, targetMatched)
		actorMatched = subtle.ConstantTimeSelect(actorEqual, 1, actorMatched)
		matchedIndex = subtle.ConstantTimeSelect(actorEqual, index, matchedIndex)
	}
	if err := activeContext(ctx); err != nil {
		return issuerdomain.Subject{}, err
	}
	if credentialMatched != 1 {
		return issuerdomain.Subject{}, subjectRejected(issuerdomain.DiagnosticSTSSubjectRejected)
	}
	if targetMatched != 1 {
		return issuerdomain.Subject{}, subjectRejected(issuerdomain.DiagnosticSTSTargetRejected)
	}
	if actorMatched != 1 {
		return issuerdomain.Subject{}, subjectRejected(issuerdomain.DiagnosticSTSActorRejected)
	}

	subject := v.subjects[matchedIndex].subject.Clone()
	if len(credential.Scopes) == 0 {
		return subject, nil
	}
	if !scopeSubset(credential.Scopes, subject.Scopes, v.options) {
		return issuerdomain.Subject{}, subjectRejected(issuerdomain.DiagnosticSTSTargetRejected)
	}
	subject.Scopes = append([]string(nil), credential.Scopes...)
	return subject, nil
}

func buildRefreshRecord(registration RefreshRegistration, options CredentialOptions) (refreshRecord, [sha256.Size * 3]byte, error) {
	var key [sha256.Size * 3]byte
	if !boundedStringCredential(registration.ClientID, options.MaxCredentialBytes) ||
		!boundedStringCredential(registration.ClientSecret, options.MaxCredentialBytes) ||
		!boundedStringCredential(registration.RefreshToken, options.MaxCredentialBytes) {
		return refreshRecord{}, key, fixtureConfigError()
	}
	subject, err := fixtureSubject(registration.Identity, registration.Scopes, options)
	if err != nil {
		return refreshRecord{}, key, err
	}
	record := refreshRecord{
		clientID:     sha256.Sum256([]byte(registration.ClientID)),
		clientSecret: sha256.Sum256([]byte(registration.ClientSecret)),
		refreshToken: sha256.Sum256([]byte(registration.RefreshToken)),
		subject:      subject,
	}
	copy(key[0:sha256.Size], record.clientID[:])
	copy(key[sha256.Size:sha256.Size*2], record.clientSecret[:])
	copy(key[sha256.Size*2:], record.refreshToken[:])
	return record, key, nil
}

func buildSubjectRecord(registration SubjectRegistration, options CredentialOptions) (subjectRecord, [sha256.Size * 6]byte, error) {
	var key [sha256.Size * 6]byte
	if !validTokenType(registration.TokenType, options.MaxTokenTypeBytes) ||
		!boundedStringCredential(registration.Token, options.MaxCredentialBytes) ||
		!boundedText(registration.Audience, options.MaxTargetBytes, true) ||
		!boundedText(registration.Resource, options.MaxTargetBytes, true) ||
		(registration.ActorTokenType == "") != (registration.ActorToken == "") {
		return subjectRecord{}, key, fixtureConfigError()
	}
	if registration.ActorTokenType != "" && (!validTokenType(registration.ActorTokenType, options.MaxTokenTypeBytes) ||
		!boundedStringCredential(registration.ActorToken, options.MaxCredentialBytes)) {
		return subjectRecord{}, key, fixtureConfigError()
	}
	subject, err := fixtureSubject(registration.Identity, registration.Scopes, options)
	if err != nil {
		return subjectRecord{}, key, err
	}
	record := subjectRecord{
		tokenType:      sha256.Sum256([]byte(registration.TokenType)),
		token:          sha256.Sum256([]byte(registration.Token)),
		audience:       sha256.Sum256([]byte(registration.Audience)),
		resource:       sha256.Sum256([]byte(registration.Resource)),
		actorTokenType: sha256.Sum256([]byte(registration.ActorTokenType)),
		actorToken:     sha256.Sum256([]byte(registration.ActorToken)),
		hasActor:       registration.ActorTokenType != "",
		subject:        subject,
	}
	segments := [][sha256.Size]byte{record.tokenType, record.token, record.audience, record.resource, record.actorTokenType, record.actorToken}
	for index := range segments {
		copy(key[index*sha256.Size:(index+1)*sha256.Size], segments[index][:])
	}
	return record, key, nil
}

func fixtureSubject(identity string, scopes []string, options CredentialOptions) (issuerdomain.Subject, error) {
	if !boundedText(identity, options.MaxIdentityBytes, false) ||
		!validScopeSet(scopes, options.MaxScopes, options.MaxScopeBytes) {
		return issuerdomain.Subject{}, fixtureConfigError()
	}
	return issuerdomain.Subject{
		PrincipalDigest: issuerdomain.Digest([]byte(identity)),
		Scopes:          append([]string(nil), scopes...),
	}, nil
}

func validateCredentialOptions(options CredentialOptions) error {
	if options.MaxRegistrations < 1 || options.MaxCredentialBytes < 1 ||
		options.MaxIdentityBytes < 1 || options.MaxTokenTypeBytes < 1 ||
		options.MaxTargetBytes < 1 || options.MaxScopes < 1 || options.MaxScopeBytes < 1 {
		return fixtureConfigError()
	}
	return nil
}

func validScopeSet(scopes []string, maxCount, maxBytes int) bool {
	if len(scopes) > maxCount {
		return false
	}
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		if len(scope) > maxBytes || !issuerdomain.ValidScopeToken(scope) {
			return false
		}
		if _, duplicate := seen[scope]; duplicate {
			return false
		}
		seen[scope] = struct{}{}
	}
	return true
}

func scopeSubset(requested, allowed []string, options CredentialOptions) bool {
	if !validScopeSet(requested, options.MaxScopes, options.MaxScopeBytes) {
		return false
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, scope := range allowed {
		allowedSet[scope] = struct{}{}
	}
	for _, scope := range requested {
		if _, ok := allowedSet[scope]; !ok {
			return false
		}
	}
	return true
}

func validTokenType(value string, maxBytes int) bool {
	if !boundedText(value, maxBytes, false) {
		return false
	}
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme != "" && parsed.Fragment == "" && parsed.User == nil
}

func boundedStringCredential(value string, maxBytes int) bool {
	return boundedCredential([]byte(value), maxBytes)
}

func boundedCredential(value []byte, maxBytes int) bool {
	if len(value) == 0 || len(value) > maxBytes || !utf8.Valid(value) {
		return false
	}
	for _, character := range string(value) {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func boundedText(value string, maxBytes int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func activeContext(ctx context.Context) error {
	if ctx == nil {
		return issuerdomain.NewError(issuerdomain.ErrorServer, issuerdomain.DiagnosticContextEnded, nil)
	}
	if err := ctx.Err(); err != nil {
		return issuerdomain.NewError(issuerdomain.ErrorServer, issuerdomain.DiagnosticContextEnded, err)
	}
	return nil
}

func fixtureConfigError() error {
	return issuerdomain.NewError(issuerdomain.ErrorServer, issuerdomain.DiagnosticFixtureConfig, nil)
}

func refreshRejected() error {
	return issuerdomain.NewError(issuerdomain.ErrorInvalidGrant, issuerdomain.DiagnosticRefreshRejected, nil)
}

func subjectRejected(diagnostic issuerdomain.Diagnostic) error {
	return issuerdomain.NewError(issuerdomain.ErrorInvalidGrant, diagnostic, nil)
}

var _ issuerports.CredentialVerifier = (*CredentialVerifier)(nil)
