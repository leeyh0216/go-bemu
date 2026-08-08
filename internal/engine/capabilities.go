package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

type TransactionScope string

const (
	TransactionScopeSingleTable TransactionScope = "single-table"
	TransactionScopeMultiTable  TransactionScope = "multi-table"
)

type AtomicReplacementScope string

const (
	AtomicReplacementTable     AtomicReplacementScope = "table"
	AtomicReplacementPartition AtomicReplacementScope = "partition"
)

type InspectionScope string

const (
	InspectionDatasets   InspectionScope = "datasets"
	InspectionTables     InspectionScope = "tables"
	InspectionTableShape InspectionScope = "table-shape"
)

type DDLOperation string

const (
	DDLCreateTable      DDLOperation = "create-table"
	DDLDropTable        DDLOperation = "drop-table"
	DDLAddColumn        DDLOperation = "add-column"
	DDLDropColumn       DDLOperation = "drop-column"
	DDLRenameColumn     DDLOperation = "rename-column"
	DDLChangeColumnType DDLOperation = "change-column-type"
)

type DDLAtomicity string

const (
	DDLAtomicityStatement DDLAtomicity = "statement"
	DDLAtomicityTable     DDLAtomicity = "table"
)

// DDLCapability describes the strongest atomicity and deepest logical field
// path one DDL operation accepts. Table create/drop use depth zero. ALTER
// operations require a positive depth; one means top-level fields only.
type DDLCapability struct {
	Atomicity         DDLAtomicity
	MaxFieldPathDepth int
}

// DecimalCapabilities describes logical decimal bounds. Physical type names
// and mappings remain private to an engine adapter.
type DecimalCapabilities struct {
	Supported    bool
	MaxPrecision int64
	MaxScale     int64
}

// CompositeCapabilities bound recursive logical shapes. A zero depth means the
// corresponding logical shape is unsupported.
type CompositeCapabilities struct {
	MaxStructDepth int
	MaxListDepth   int
}

// CapabilitiesDescriptor is mutable input to NewCapabilities and output from
// Descriptor. Capabilities itself owns deep copies of every map.
type CapabilitiesDescriptor struct {
	Identity           Identity
	Decimal            DecimalCapabilities
	Composite          CompositeCapabilities
	Transactions       map[TransactionScope]bool
	AtomicReplacements map[AtomicReplacementScope]bool
	Inspection         map[InspectionScope]bool
	DDL                map[DDLOperation]DDLCapability
}

// Capabilities is an immutable snapshot published by one engine runtime.
// Consumers query it through methods or request an explicitly detached
// descriptor copy.
type Capabilities struct {
	identity           Identity
	decimal            DecimalCapabilities
	composite          CompositeCapabilities
	transactions       map[TransactionScope]struct{}
	atomicReplacements map[AtomicReplacementScope]struct{}
	inspection         map[InspectionScope]struct{}
	ddl                map[DDLOperation]DDLCapability
	fingerprint        string
}

func NewCapabilities(descriptor CapabilitiesDescriptor) (Capabilities, error) {
	if err := descriptor.Identity.validate(); err != nil {
		return Capabilities{}, err
	}
	if (!descriptor.Decimal.Supported &&
		(descriptor.Decimal.MaxPrecision != 0 || descriptor.Decimal.MaxScale != 0)) ||
		(descriptor.Decimal.Supported &&
			(descriptor.Decimal.MaxPrecision < 1 || descriptor.Decimal.MaxScale < 0 ||
				descriptor.Decimal.MaxScale > descriptor.Decimal.MaxPrecision)) {
		return Capabilities{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "capabilities", "decimal", "decimal bounds are invalid", nil,
		)
	}
	if descriptor.Composite.MaxStructDepth < 0 || descriptor.Composite.MaxListDepth < 0 {
		return Capabilities{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "capabilities", "composite", "composite depth cannot be negative", nil,
		)
	}

	capabilities := Capabilities{
		identity: descriptor.Identity, decimal: descriptor.Decimal, composite: descriptor.Composite,
		transactions:       make(map[TransactionScope]struct{}),
		atomicReplacements: make(map[AtomicReplacementScope]struct{}),
		inspection:         make(map[InspectionScope]struct{}),
		ddl:                make(map[DDLOperation]DDLCapability),
	}
	for scope, supported := range descriptor.Transactions {
		if !validTransactionScope(scope) {
			return Capabilities{}, invalidCapabilityDescriptor("transaction", string(scope))
		}
		if supported {
			capabilities.transactions[scope] = struct{}{}
		}
	}
	for scope, supported := range descriptor.AtomicReplacements {
		if !validAtomicReplacementScope(scope) {
			return Capabilities{}, invalidCapabilityDescriptor("atomic-replacement", string(scope))
		}
		if supported {
			capabilities.atomicReplacements[scope] = struct{}{}
		}
	}
	for scope, supported := range descriptor.Inspection {
		if !validInspectionScope(scope) {
			return Capabilities{}, invalidCapabilityDescriptor("inspection", string(scope))
		}
		if supported {
			capabilities.inspection[scope] = struct{}{}
		}
	}
	for operation, ddl := range descriptor.DDL {
		if !validDDLOperation(operation) || !validDDLAtomicity(ddl.Atomicity) ||
			ddl.MaxFieldPathDepth < 0 || (ddlUsesFieldPath(operation) && ddl.MaxFieldPathDepth < 1) ||
			(!ddlUsesFieldPath(operation) && ddl.MaxFieldPathDepth != 0) {
			return Capabilities{}, invalidCapabilityDescriptor("ddl", string(operation))
		}
		capabilities.ddl[operation] = ddl
	}
	capabilities.fingerprint = capabilitiesFingerprint(capabilities)
	return capabilities, nil
}

func (capabilities Capabilities) Identity() Identity { return capabilities.identity }

func (capabilities Capabilities) Decimal() DecimalCapabilities { return capabilities.decimal }

func (capabilities Capabilities) Composite() CompositeCapabilities { return capabilities.composite }

func (capabilities Capabilities) Fingerprint() string { return capabilities.fingerprint }

func (capabilities Capabilities) SupportsTransaction(scope TransactionScope) bool {
	_, supported := capabilities.transactions[scope]
	return supported
}

func (capabilities Capabilities) SupportsAtomicReplacement(scope AtomicReplacementScope) bool {
	_, supported := capabilities.atomicReplacements[scope]
	return supported
}

func (capabilities Capabilities) SupportsInspection(scope InspectionScope) bool {
	_, supported := capabilities.inspection[scope]
	return supported
}

func (capabilities Capabilities) DDL(operation DDLOperation) (DDLCapability, bool) {
	ddl, supported := capabilities.ddl[operation]
	return ddl, supported
}

func (capabilities Capabilities) Descriptor() CapabilitiesDescriptor {
	descriptor := CapabilitiesDescriptor{
		Identity: capabilities.identity, Decimal: capabilities.decimal, Composite: capabilities.composite,
		Transactions:       make(map[TransactionScope]bool, len(capabilities.transactions)),
		AtomicReplacements: make(map[AtomicReplacementScope]bool, len(capabilities.atomicReplacements)),
		Inspection:         make(map[InspectionScope]bool, len(capabilities.inspection)),
		DDL:                make(map[DDLOperation]DDLCapability, len(capabilities.ddl)),
	}
	for scope := range capabilities.transactions {
		descriptor.Transactions[scope] = true
	}
	for scope := range capabilities.atomicReplacements {
		descriptor.AtomicReplacements[scope] = true
	}
	for scope := range capabilities.inspection {
		descriptor.Inspection[scope] = true
	}
	for operation, ddl := range capabilities.ddl {
		descriptor.DDL[operation] = ddl
	}
	return descriptor
}

func (capabilities Capabilities) validate() error {
	if err := capabilities.identity.validate(); err != nil {
		return err
	}
	if capabilities.fingerprint == "" || capabilities.fingerprint != capabilitiesFingerprint(capabilities) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "capabilities", "capability.fingerprint", "zero or inconsistent capability snapshot", nil,
		)
	}
	return nil
}

func invalidCapabilityDescriptor(kind, value string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, "capabilities", "capability."+kind,
		fmt.Sprintf("unknown %s capability %q", kind, value), nil,
	)
}

func validTransactionScope(scope TransactionScope) bool {
	return scope == TransactionScopeSingleTable || scope == TransactionScopeMultiTable
}

func validAtomicReplacementScope(scope AtomicReplacementScope) bool {
	return scope == AtomicReplacementTable || scope == AtomicReplacementPartition
}

func validInspectionScope(scope InspectionScope) bool {
	return scope == InspectionDatasets || scope == InspectionTables || scope == InspectionTableShape
}

func validDDLOperation(operation DDLOperation) bool {
	switch operation {
	case DDLCreateTable, DDLDropTable, DDLAddColumn, DDLDropColumn, DDLRenameColumn, DDLChangeColumnType:
		return true
	default:
		return false
	}
}

func validDDLAtomicity(atomicity DDLAtomicity) bool {
	return atomicity == DDLAtomicityStatement || atomicity == DDLAtomicityTable
}

func ddlUsesFieldPath(operation DDLOperation) bool {
	switch operation {
	case DDLAddColumn, DDLDropColumn, DDLRenameColumn, DDLChangeColumnType:
		return true
	default:
		return false
	}
}

type capabilitiesFingerprintDocument struct {
	EngineID           string                `json:"engineId"`
	EngineVersion      string                `json:"engineVersion"`
	Decimal            DecimalCapabilities   `json:"decimal"`
	Composite          CompositeCapabilities `json:"composite"`
	Transactions       []string              `json:"transactions"`
	AtomicReplacements []string              `json:"atomicReplacements"`
	Inspection         []string              `json:"inspection"`
	DDL                []ddlFingerprintEntry `json:"ddl"`
}

type ddlFingerprintEntry struct {
	Operation         string `json:"operation"`
	Atomicity         string `json:"atomicity"`
	MaxFieldPathDepth int    `json:"maxFieldPathDepth"`
}

func capabilitiesFingerprint(capabilities Capabilities) string {
	document := capabilitiesFingerprintDocument{
		EngineID: capabilities.identity.ID(), EngineVersion: capabilities.identity.Version(),
		Decimal: capabilities.decimal, Composite: capabilities.composite,
	}
	for scope := range capabilities.transactions {
		document.Transactions = append(document.Transactions, string(scope))
	}
	for scope := range capabilities.atomicReplacements {
		document.AtomicReplacements = append(document.AtomicReplacements, string(scope))
	}
	for scope := range capabilities.inspection {
		document.Inspection = append(document.Inspection, string(scope))
	}
	for operation, ddl := range capabilities.ddl {
		document.DDL = append(document.DDL, ddlFingerprintEntry{
			Operation: string(operation), Atomicity: string(ddl.Atomicity), MaxFieldPathDepth: ddl.MaxFieldPathDepth,
		})
	}
	sort.Strings(document.Transactions)
	sort.Strings(document.AtomicReplacements)
	sort.Strings(document.Inspection)
	sort.Slice(document.DDL, func(left, right int) bool {
		return document.DDL[left].Operation < document.DDL[right].Operation
	})
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(fmt.Sprintf("marshal engine capability fingerprint: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}
