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

// PlanningError carries stable planning classification and the original cause.
type PlanningError struct {
	code      PlanningErrorCode
	operation string
	attribute string
	detail    string
	cause     error
}

func newPlanningError(code PlanningErrorCode, operation, attribute, detail string, cause error) *PlanningError {
	return &PlanningError{code: code, operation: operation, attribute: attribute, detail: detail, cause: cause}
}

func (err *PlanningError) Error() string {
	if err == nil {
		return "<nil>"
	}
	message := fmt.Sprintf(
		"engine planning failed: code=%s operation=%s attribute=%s detail=%s",
		err.code, err.operation, err.attribute, err.detail,
	)
	if err.cause != nil {
		message += fmt.Sprintf(" cause=%v", err.cause)
	}
	return message
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

func (err *PlanningError) Unwrap() []error {
	if err == nil {
		return nil
	}
	causes := make([]error, 0, 2)
	switch err.code {
	case PlanningCodeInvalidDescriptor:
		causes = append(causes, domain.ErrInvalid)
	case PlanningCodeUnsupported:
		causes = append(causes, domain.ErrUnsupported)
	case PlanningCodeEngineMismatch, PlanningCodeMutationMismatch, PlanningCodeCapabilityDrift,
		PlanningCodePlannerMismatch, PlanningCodePhysicalStateDrift:
		causes = append(causes, domain.ErrPrecondition)
	}
	if err.cause != nil {
		causes = append(causes, err.cause)
	}
	return causes
}
