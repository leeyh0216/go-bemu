package domain

import (
	"fmt"
	"reflect"
)

type MutationPhase string

const (
	MutationPrepared MutationPhase = "PREPARED"
	MutationPhysical MutationPhase = "PHYSICAL"
	MutationApplied  MutationPhase = "APPLIED"
	MutationAborted  MutationPhase = "ABORTED"
)

type MutationPublication string

const (
	MutationPublishNone         MutationPublication = "NONE"
	MutationPublishCreate       MutationPublication = "CREATE"
	MutationPublishSchemaUpdate MutationPublication = "SCHEMA_UPDATE"
)

type MutationResult struct {
	OutputRows         int64
	CreatedDestination bool
	UpdatedDestination bool
}

type MutationRecord struct {
	ID                  string
	Job                 JobReference
	ConfigurationDigest string
	PlanFingerprint     string
	Destination         Table
	BeforeSchema        []Field
	Publication         MutationPublication
	Phase               MutationPhase
	InputFiles          int64
	InputBytes          int64
	Result              *MutationResult
}

func (record MutationRecord) Validate() error {
	if err := validateReference(record.Job); err != nil {
		return err
	}
	wantID, err := LoadMutationID(record.Job, record.ConfigurationDigest)
	if err != nil || record.ID != wantID {
		return fmt.Errorf("%w: load mutation identity does not match its job", ErrInvalid)
	}
	if !ValidLoadMutationID(record.PlanFingerprint) {
		return fmt.Errorf("%w: load mutation plan fingerprint is invalid", ErrInvalid)
	}
	if err := ValidateTable(record.Destination); err != nil {
		return err
	}
	if record.InputFiles < 0 || record.InputBytes < 0 {
		return fmt.Errorf("%w: load mutation statistics must not be negative", ErrInvalid)
	}
	if err := ValidateSchema(record.BeforeSchema); err != nil {
		return err
	}
	switch record.Publication {
	case MutationPublishNone:
	case MutationPublishCreate:
		if len(record.BeforeSchema) != 0 {
			return fmt.Errorf("%w: destination creation cannot have a prior schema", ErrInvalid)
		}
	case MutationPublishSchemaUpdate:
		if len(record.BeforeSchema) == 0 || reflect.DeepEqual(record.BeforeSchema, record.Destination.Schema) {
			return fmt.Errorf("%w: schema publication requires distinct prior and destination schemas", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: load mutation publication is invalid", ErrInvalid)
	}
	switch record.Phase {
	case MutationPrepared:
		if record.Result != nil {
			return fmt.Errorf("%w: prepared load mutation cannot have a physical result", ErrInvalid)
		}
	case MutationPhysical, MutationApplied:
		if err := validateMutationResult(record); err != nil {
			return err
		}
	case MutationAborted:
		if record.Result != nil {
			return fmt.Errorf("%w: aborted load mutation cannot retain a physical result", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: load mutation phase is invalid", ErrInvalid)
	}
	return nil
}

func (record MutationRecord) Clone() MutationRecord {
	clone := record
	clone.Destination = CloneTable(record.Destination)
	clone.BeforeSchema = cloneFields(record.BeforeSchema)
	if record.Result != nil {
		result := *record.Result
		clone.Result = &result
	}
	return clone
}

func validateMutationResult(record MutationRecord) error {
	if record.Result == nil || record.Result.OutputRows < 0 {
		return fmt.Errorf("%w: physical load mutation result is invalid", ErrInvalid)
	}
	wantCreated := record.Publication == MutationPublishCreate
	wantUpdated := record.Publication == MutationPublishSchemaUpdate
	if record.Result.CreatedDestination != wantCreated || record.Result.UpdatedDestination != wantUpdated {
		return fmt.Errorf("%w: physical load result does not match its publication", ErrInvalid)
	}
	return nil
}
