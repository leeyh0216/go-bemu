package domain

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type PartitionDecoratorKind string

const (
	PartitionDecoratorTime  PartitionDecoratorKind = "TIME"
	PartitionDecoratorRange PartitionDecoratorKind = "RANGE"
)

type PartitionDecorator struct {
	Kind       PartitionDecoratorKind
	ID         string
	TimeStart  time.Time
	TimeEnd    time.Time
	RangeStart int64
	RangeEnd   int64
}

var tableIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func SplitPartitionDecorator(reference TableReference) (TableReference, string, bool, error) {
	separator := strings.LastIndexByte(reference.TableID, '$')
	if separator < 0 {
		if !tableIDPattern.MatchString(reference.TableID) || len(reference.TableID) > 1024 {
			return TableReference{}, "", false, fmt.Errorf("%w: invalid destination tableId %q", ErrInvalid, reference.TableID)
		}
		return reference, "", false, nil
	}
	if separator == 0 || separator == len(reference.TableID)-1 || strings.Contains(reference.TableID[:separator], "$") {
		return TableReference{}, "", false, fmt.Errorf("%w: invalid destination partition decorator", ErrInvalid)
	}
	base := reference
	base.TableID = reference.TableID[:separator]
	if !tableIDPattern.MatchString(base.TableID) || len(base.TableID) > 1024 {
		return TableReference{}, "", false, fmt.Errorf("%w: invalid destination tableId %q", ErrInvalid, base.TableID)
	}
	partitionID := reference.TableID[separator+1:]
	for index, character := range partitionID {
		if index == 0 && character == '-' && len(partitionID) > 1 {
			continue
		}
		if character < '0' || character > '9' {
			return TableReference{}, "", false, fmt.Errorf("%w: partition decorator must contain decimal digits", ErrInvalid)
		}
	}
	return base, partitionID, true, nil
}

func ResolvePartitionDecorator(partitionID string, table Table) (*PartitionDecorator, error) {
	if table.TimePartitioning != nil {
		layout, err := partitionTimeLayout(table.TimePartitioning.Type)
		if err != nil {
			return nil, err
		}
		start, err := time.ParseInLocation(layout, partitionID, time.UTC)
		if err != nil || start.Format(layout) != partitionID {
			return nil, fmt.Errorf("%w: partition decorator does not match %s partitioning", ErrInvalid, strings.ToUpper(table.TimePartitioning.Type))
		}
		return &PartitionDecorator{
			Kind: PartitionDecoratorTime, ID: partitionID, TimeStart: start, TimeEnd: nextPartitionTime(start, table.TimePartitioning.Type),
		}, nil
	}
	if table.RangePartitioning != nil {
		start, err := strconv.ParseInt(partitionID, 10, 64)
		partitioning := table.RangePartitioning
		if err != nil || start < partitioning.Range.Start || start >= partitioning.Range.End ||
			(start-partitioning.Range.Start)%partitioning.Range.Interval != 0 {
			return nil, fmt.Errorf("%w: partition decorator is not an integer-range partition boundary", ErrInvalid)
		}
		end := start + partitioning.Range.Interval
		if end > partitioning.Range.End {
			end = partitioning.Range.End
		}
		return &PartitionDecorator{Kind: PartitionDecoratorRange, ID: partitionID, RangeStart: start, RangeEnd: end}, nil
	}
	return nil, fmt.Errorf("%w: partition decorator requires a partitioned destination", ErrPrecondition)
}

func ValidatePartitionDecorator(decorator *PartitionDecorator, table Table) error {
	if decorator == nil {
		return nil
	}
	resolved, err := ResolvePartitionDecorator(decorator.ID, table)
	if err != nil {
		return err
	}
	if *resolved != *decorator {
		return fmt.Errorf("%w: partition decorator does not match the destination layout", ErrPrecondition)
	}
	return nil
}

func partitionTimeLayout(partitionType string) (string, error) {
	switch strings.ToUpper(partitionType) {
	case "HOUR":
		return "2006010215", nil
	case "DAY":
		return "20060102", nil
	case "MONTH":
		return "200601", nil
	case "YEAR":
		return "2006", nil
	default:
		return "", fmt.Errorf("%w: invalid time partitioning type %q", ErrInvalid, partitionType)
	}
}

func nextPartitionTime(start time.Time, partitionType string) time.Time {
	switch strings.ToUpper(partitionType) {
	case "HOUR":
		return start.Add(time.Hour)
	case "DAY":
		return start.AddDate(0, 0, 1)
	case "MONTH":
		return start.AddDate(0, 1, 0)
	case "YEAR":
		return start.AddDate(1, 0, 0)
	default:
		return time.Time{}
	}
}
