package engine

import (
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type PlanningErrorCode string

const (
	PlanningCodeInvalidDescriptor  PlanningErrorCode = "ENGINE_PLAN_INVALID_DESCRIPTOR"
	PlanningCodeUnsupported        PlanningErrorCode = "ENGINE_PLAN_UNSUPPORTED"
	PlanningCodeEngineMismatch     PlanningErrorCode = "ENGINE_PLAN_ENGINE_MISMATCH"
	PlanningCodeMutationMismatch   PlanningErrorCode = "ENGINE_PLAN_MUTATION_MISMATCH"
	PlanningCodeCapabilityDrift    PlanningErrorCode = "ENGINE_PLAN_CAPABILITY_DRIFT"
	PlanningCodePlannerMismatch    PlanningErrorCode = "ENGINE_PLAN_PLANNER_MISMATCH"
	PlanningCodePhysicalStateDrift PlanningErrorCode = "ENGINE_PLAN_PHYSICAL_STATE_DRIFT"
)

// PlanningError is safe to carry across the application boundary. Attribute
// contains a stable logical capability name; Detail must not contain SQL,
// connection strings, physical type names, or user values.
type PlanningError struct {
	code      PlanningErrorCode
	operation string
	attribute string
	detail    string
}

func newPlanningError(code PlanningErrorCode, operation, attribute, detail string, _ error) *PlanningError {
	return &PlanningError{code: code, operation: operation, attribute: attribute, detail: detail}
}

func (err *PlanningError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"engine planning failed: code=%s operation=%s attribute=%s detail=%s",
		err.code, err.operation, err.attribute, err.detail,
	)
}

func (err *PlanningError) Code() PlanningErrorCode {
	if err == nil {
		return ""
	}
	return err.code
}

func (err *PlanningError) Operation() string {
	if err == nil {
		return ""
	}
	return err.operation
}

func (err *PlanningError) Attribute() string {
	if err == nil {
		return ""
	}
	return err.attribute
}

func (err *PlanningError) Detail() string {
	if err == nil {
		return ""
	}
	return err.detail
}

// Unwrap exposes only cataloged, engine-neutral domain classifications. Raw
// adapter causes are deliberately never retained by PlanningError.
func (err *PlanningError) Unwrap() error {
	if err == nil {
		return nil
	}
	switch err.code {
	case PlanningCodeInvalidDescriptor:
		return domain.ErrInvalid
	case PlanningCodeUnsupported:
		return domain.ErrUnsupported
	case PlanningCodeEngineMismatch, PlanningCodeMutationMismatch, PlanningCodeCapabilityDrift,
		PlanningCodePlannerMismatch, PlanningCodePhysicalStateDrift:
		return domain.ErrPrecondition
	default:
		return nil
	}
}
