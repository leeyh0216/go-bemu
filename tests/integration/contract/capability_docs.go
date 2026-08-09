package integrationcontract

import (
	"fmt"
	"strings"
)

func renderCapabilityCoverage(index CapabilityIndex, language string) []byte {
	var output strings.Builder
	if language == "ko" {
		output.WriteString("<!-- doc-id: integration-capability-coverage -->\n<!-- lang: ko -->\n\n[English](../en/capability-coverage.md) | [한국어](capability-coverage.md)\n\n")
		output.WriteString("# Spark BigQuery Connector Coverage\n\n")
		output.WriteString("이 표의 version-bound claim은 [고정 source revision](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92)을 기준으로 합니다.\n\n")
		output.WriteString("> 생성 파일입니다. 테스트의 `contract_case(...)` annotation을 수정한 뒤 `make integration-contract-generate`를 실행하세요.\n\n")
		output.WriteString("아래 표에 없는 동작은 지원 claim이 아닙니다. `verified`는 해당 공개 테스트가 통과했고, `partial`은 표에 적힌 제한 안에서만 동작합니다.\n\n")
		output.WriteString("<!-- section: claims -->\n## 테스트 기반 Claim\n\n")
		output.WriteString("| 상태 | 동작 | 검증 경로 |\n| --- | --- | --- |\n")
		for _, capability := range index.Cases {
			output.WriteString(fmt.Sprintf("| `%s` | %s | `%s` |\n", capability.State, capability.Summary, capability.ID))
			if capability.State == CapabilityCasePartial {
				output.WriteString(fmt.Sprintf("| 제한 | %s ([issue](%s)) |  |\n", capability.Limitation, capability.Issue))
			}
		}
		output.WriteString("<!-- section: api-coverage -->\n## 공개 API 범위\n\n")
		output.WriteString("| API/RPC | 테스트 기반 claim 수 |\n| --- | --- |\n")
		for _, coverage := range index.APICoverage {
			output.WriteString(fmt.Sprintf("| `%s` | %d |\n", coverage.OperationID, len(coverage.CaseIDs)))
		}
		output.WriteString("\n전체 claim-to-API mapping은 `tests/integration/contract/capabilities.normalized.json`에 있습니다. 프로필과 golden은 source-reviewed wire 계약입니다. 현재 CI의 실제 실행 trace는 이를 자동 비교하지 않으며, 실행별 evidence artifact로만 보관됩니다.\n")
	} else {
		output.WriteString("<!-- doc-id: integration-capability-coverage -->\n<!-- lang: en -->\n\n[English](capability-coverage.md) | [한국어](../ko/capability-coverage.md)\n\n")
		output.WriteString("# Spark BigQuery Connector Coverage\n\n")
		output.WriteString("Version-bound claims in this table use the [pinned source revision](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92).\n\n")
		output.WriteString("> Generated from test-local `contract_case(...)` annotations. Edit the test, then run `make integration-contract-generate`.\n\n")
		output.WriteString("Only behaviors in this table are support claims. `verified` has a passing public test; `partial` works only within its stated limit.\n\n")
		output.WriteString("<!-- section: claims -->\n## Test-Backed Claims\n\n")
		output.WriteString("| State | Behavior | Claim |\n| --- | --- | --- |\n")
		for _, capability := range index.Cases {
			output.WriteString(fmt.Sprintf("| `%s` | %s | `%s` |\n", capability.State, capability.Summary, capability.ID))
			if capability.State == CapabilityCasePartial {
				output.WriteString(fmt.Sprintf("| Limit | %s ([issue](%s)) |  |\n", capability.Limitation, capability.Issue))
			}
		}
		output.WriteString("<!-- section: api-coverage -->\n## Public API Coverage\n\n")
		output.WriteString("| API/RPC | Test-backed claims |\n| --- | --- |\n")
		for _, coverage := range index.APICoverage {
			output.WriteString(fmt.Sprintf("| `%s` | %d |\n", coverage.OperationID, len(coverage.CaseIDs)))
		}
		output.WriteString("\nThe complete claim-to-API mapping is in `tests/integration/contract/capabilities.normalized.json`. Profiles and goldens are source-reviewed wire contracts. CI runtime traces are not compared to them automatically today; they are retained as per-run evidence artifacts.\n")
	}
	return []byte(output.String())
}
