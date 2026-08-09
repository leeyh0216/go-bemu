package domain

import (
	"errors"
	"testing"
	"time"
)

func TestQueryParametersValidateModeNamesAndTypedValues(t *testing.T) {
	reference := JobReference{ProjectID: "test-project", Location: "US", JobID: "parameters"}
	valid := QueryConfiguration{SQL: "SELECT @value", ParameterMode: QueryParameterNamed, QueryParameters: []QueryParameter{{Name: "value", Type: "int64", Value: "42"}}}
	job, err := NewConfiguredQueryJob(reference, valid, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if job.Configuration.QueryParameters[0].Type != "INT64" {
		t.Fatalf("type=%q", job.Configuration.QueryParameters[0].Type)
	}
	for _, configuration := range []QueryConfiguration{
		{SQL: "SELECT @value", ParameterMode: QueryParameterNamed, QueryParameters: []QueryParameter{{Name: "value", Type: "INT64", Value: "nope"}}},
		{SQL: "SELECT @value", ParameterMode: QueryParameterNamed, QueryParameters: []QueryParameter{{Name: "value", Type: "INT64", Value: "1"}, {Name: "VALUE", Type: "INT64", Value: "2"}}},
		{SQL: "SELECT ?", ParameterMode: QueryParameterPositional, QueryParameters: []QueryParameter{{Name: "value", Type: "INT64", Value: "1"}}},
	} {
		if _, err := NewConfiguredQueryJob(reference, configuration, time.Unix(0, 0)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("configuration %#v error=%v", configuration, err)
		}
	}
}
