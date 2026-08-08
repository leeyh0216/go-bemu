package engine

import (
	"fmt"
	"regexp"
)

var (
	identityPartPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,127}$`)
	versionPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)
)

// Identity names one physical engine implementation and the implementation
// version whose behavior was used to produce a plan. It deliberately carries
// no connection details or backend SQL dialect information.
type Identity struct {
	id      string
	version string
}

func NewIdentity(id, version string) (Identity, error) {
	if !identityPartPattern.MatchString(id) {
		return Identity{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "identity", "engine.id", "engine ID is missing or invalid", nil,
		)
	}
	if !versionPattern.MatchString(version) {
		return Identity{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "identity", "engine.version", "engine version is missing or invalid", nil,
		)
	}
	return Identity{id: id, version: version}, nil
}

func (identity Identity) ID() string      { return identity.id }
func (identity Identity) Version() string { return identity.version }

func (identity Identity) validate() error {
	if !identityPartPattern.MatchString(identity.id) || !versionPattern.MatchString(identity.version) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "identity", "engine.identity", "zero or invalid engine identity", nil,
		)
	}
	return nil
}

func (identity Identity) key() string {
	return fmt.Sprintf("%s\x00%s", identity.id, identity.version)
}
