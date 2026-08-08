package enginetest

import "testing"

func TestFakeEnginePlanningConformance(t *testing.T) {
	RunPlanningConformance(t, func(testing.TB) PlanningAdapter { return NewFakeEngine() })
}
