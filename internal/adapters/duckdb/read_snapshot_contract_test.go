package duckdb

// This file freezes protocol mappings separately from DuckDB materialization.
// A connector/protobuf/Arrow dependency upgrade should fail at the exact field
// whose reference schema or row framing changed.
//
// Protocol sources:
//   - Arrow schema details: https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details
//   - Avro schema details: https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details
//   - Avro binary encoding: https://avro.apache.org/docs/1.11.4/specification/#binary-encoding

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

func TestArrowReferenceSchemaPreservesOfficialTypesModesOrderAndMetadata(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	fields := readContractFields()
	_, serialized, err := buildArrowReferenceSchema(fields)
	if err != nil {
		t.Fatal(err)
	}
	assertSingleArrowIPCMessage(t, serialized, ipc.MessageSchema)
	stream := bytes.NewBuffer(append(slices.Clone(serialized), []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}...))
	reader, err := ipc.NewReader(stream)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
	schema := reader.Schema()
	if len(schema.Fields()) != len(fields) {
		t.Fatalf("Arrow fields = %d, want %d", len(schema.Fields()), len(fields))
	}
	wantTypes := []arrow.Type{
		arrow.BOOL, arrow.INT64, arrow.FLOAT64, arrow.DECIMAL128, arrow.DECIMAL256,
		arrow.STRING, arrow.BINARY, arrow.DATE32, arrow.TIMESTAMP, arrow.TIME64,
		arrow.TIMESTAMP, arrow.STRING, arrow.STRUCT, arrow.LIST,
	}
	for index, want := range wantTypes {
		got := schema.Field(index)
		if got.Name != fields[index].Name || got.Type.ID() != want {
			t.Errorf("field %d = %s/%s, want %s/%s", index, got.Name, got.Type.ID(), fields[index].Name, want)
		}
		if bigQueryType, ok := got.Metadata.GetValue("BIGQUERY:type"); !ok || bigQueryType != strings.ToUpper(fields[index].Type) {
			t.Errorf("field %q BIGQUERY:type = %q/%v", got.Name, bigQueryType, ok)
		}
		if mode, ok := got.Metadata.GetValue("BIGQUERY:mode"); !ok || mode != normalizedFieldMode(fields[index]) {
			t.Errorf("field %q BIGQUERY:mode = %q/%v", got.Name, mode, ok)
		}
	}
	if schema.Field(0).Nullable {
		t.Fatal("REQUIRED BOOL unexpectedly nullable")
	}
	if !schema.Field(1).Nullable {
		t.Fatal("default mode must be nullable")
	}
	list, ok := schema.Field(13).Type.(*arrow.ListType)
	if !ok || schema.Field(13).Nullable || list.ElemField().Nullable {
		t.Fatalf("REPEATED mapping = %#v; want non-null list and element", schema.Field(13))
	}
	decimal128Type := schema.Field(3).Type.(*arrow.Decimal128Type)
	if decimal128Type.Precision != 38 || decimal128Type.Scale != 9 {
		t.Fatalf("NUMERIC Arrow type = %v", decimal128Type)
	}
	decimal256Type := schema.Field(4).Type.(*arrow.Decimal256Type)
	if decimal256Type.Precision != 38 || decimal256Type.Scale != 18 {
		t.Fatalf("BIGNUMERIC Arrow type = %v", decimal256Type)
	}
	dateTimeType := schema.Field(8).Type.(*arrow.TimestampType)
	if dateTimeType.Unit != arrow.Microsecond || dateTimeType.TimeZone != "" {
		t.Fatalf("DATETIME Arrow type = %v", dateTimeType)
	}
	timestampType := schema.Field(10).Type.(*arrow.TimestampType)
	if timestampType.Unit != arrow.Microsecond || timestampType.TimeZone != "UTC" {
		t.Fatalf("TIMESTAMP Arrow type = %v", timestampType)
	}
	wantExtensions := map[int]string{
		8:  "google:sqlType:datetime",
		11: "google:sqlType:json",
	}
	for index, want := range wantExtensions {
		if extension, ok := schema.Field(index).Metadata.GetValue("ARROW:extension:name"); !ok || extension != want {
			t.Errorf("field %q Arrow extension = %q/%v, want %q", fields[index].Name, extension, ok, want)
		}
	}
	if description, ok := schema.Field(0).Metadata.GetValue("BIGQUERY:description"); !ok || description != fields[0].Description {
		t.Fatalf("description metadata = %q/%v", description, ok)
	}
}

func TestAvroReferenceSchemaPreservesOfficialTypesAndNullFirstUnion(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	fields := readContractFields()
	serialized, err := buildAvroReferenceSchema(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Type   string `json:"type"`
		Name   string `json:"name"`
		Fields []struct {
			Name string          `json:"name"`
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(serialized, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Type != "record" || schema.Name != "root" || len(schema.Fields) != len(fields) {
		t.Fatalf("unexpected Avro root schema: %s", serialized)
	}
	for index, field := range schema.Fields {
		if field.Name != fields[index].Name {
			t.Errorf("Avro field %d = %q, want %q", index, field.Name, fields[index].Name)
		}
	}
	if got := string(schema.Fields[1].Type); !strings.HasPrefix(got, `["null",`) {
		t.Fatalf("nullable Avro union = %s, want null first", got)
	}
	checks := map[int][]string{
		3:  {`"logicalType":"decimal"`, `"precision":38`, `"scale":9`},
		4:  {`"logicalType":"decimal"`, `"precision":38`, `"scale":18`},
		7:  {`"logicalType":"date"`},
		8:  {`"logicalType":"datetime"`},
		9:  {`"logicalType":"time-micros"`},
		10: {`"logicalType":"timestamp-micros"`},
		11: {`"sqlType":"JSON"`},
		12: {`"type":"record"`},
		13: {`"type":"array"`},
	}
	for index, fragments := range checks {
		for _, fragment := range fragments {
			if !strings.Contains(string(schema.Fields[index].Type), fragment) {
				t.Errorf("Avro field %q lacks %s: %s", fields[index].Name, fragment, schema.Fields[index].Type)
			}
		}
	}
}

func TestReferenceSchemasPreserveExplicitDecimalParameters(t *testing.T) {
	precision20, scale4 := int64(20), int64(4)
	precision38, scale12 := int64(38), int64(12)
	fields := []catalogdomain.Field{
		{Name: "numeric_value", Type: "NUMERIC", Precision: &precision20, Scale: &scale4},
		{Name: "bignumeric_value", Type: "BIGNUMERIC", Precision: &precision38, Scale: &scale12},
	}

	arrowSchema, _, err := buildArrowReferenceSchema(fields)
	if err != nil {
		t.Fatal(err)
	}
	numericType := arrowSchema.Field(0).Type.(*arrow.Decimal128Type)
	if numericType.Precision != 20 || numericType.Scale != 4 {
		t.Fatalf("explicit NUMERIC Arrow type = %v", numericType)
	}
	bignumericType := arrowSchema.Field(1).Type.(*arrow.Decimal256Type)
	if bignumericType.Precision != 38 || bignumericType.Scale != 12 {
		t.Fatalf("explicit BIGNUMERIC Arrow type = %v", bignumericType)
	}

	avroSchema, err := buildAvroReferenceSchema(fields)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"precision":20,"scale":4`,
		`"precision":38,"scale":12`,
	} {
		if !strings.Contains(string(avroSchema), fragment) {
			t.Fatalf("Avro decimal schema lacks %s: %s", fragment, avroSchema)
		}
	}
}

func TestDuckDBReadSnapshotEmitsConcatenatedRawAvroDatums(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "avro_rows", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "name", Type: "STRING"},
			{Name: "tags", Type: "STRING", Mode: "REPEATED"},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(3, 'abc', ['x', 'yz']), (4, NULL, [])")
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))
	snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatAvro,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotPort.(*duckDBReadSnapshot)
	defer closeReadSnapshot(t, snapshot)
	metadata := snapshot.Metadata()
	if metadata.RowCount != 2 || !json.Valid(metadata.Schema.Serialized) {
		t.Fatalf("Avro metadata = %+v", metadata)
	}
	iterator, err := snapshot.OpenRange(ctx, 0, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := iterator.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	if bytes.HasPrefix(batch.SerializedRows, []byte{'O', 'b', 'j', 1}) {
		t.Fatalf("AvroRows contains an object-container header: %x", batch.SerializedRows)
	}
	// Datum 1: id=3; union branch 1 + "abc"; array block ["x","yz"].
	// Datum 2: id=4; null branch 0; empty array block 0.
	want := []byte{6, 2, 6, 'a', 'b', 'c', 4, 2, 'x', 4, 'y', 'z', 0, 8, 0, 0}
	if !bytes.Equal(batch.SerializedRows, want) {
		t.Fatalf("raw Avro datums = %x, want %x", batch.SerializedRows, want)
	}
	if _, err := iterator.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("Avro iterator end = %v, want io.EOF", err)
	}
}

func TestDuckDBReadSnapshotEncodesEverySupportedDriverTypeInBothFormats(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "all_types", Type: "TABLE",
		Schema: readContractFields(),
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	physicalTable := quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + "." + quoteIdentifier(table.ID)
	statement := "INSERT INTO " + physicalTable + " VALUES (" + strings.Join([]string{
		"TRUE",
		"-42",
		"1.25",
		"CAST('123.456789012' AS DECIMAL(38,9))",
		"CAST('1.000000000000000000' AS DECIMAL(38,18))",
		"'text-value'",
		"from_hex('00ff')",
		"DATE '2026-08-08'",
		"TIMESTAMP '2026-08-08 12:34:56.123456'",
		"TIME '12:34:56.123456'",
		"TIMESTAMPTZ '2026-08-08 12:34:56.123456+00'",
		"'{\"answer\":42}'",
		"{'child':'nested-value'}",
		"[1, 2, 3]",
	}, ", ") + ")"
	if _, err := warehouse.db.ExecContext(ctx, statement); err != nil {
		t.Fatal(err)
	}
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))
	for _, format := range []readdomain.Format{readdomain.FormatArrow, readdomain.FormatAvro} {
		format := format
		t.Run(strings.ToLower(format.String()), func(t *testing.T) {
			ctx, cancel := duckDBReadTestContext(t)
			defer cancel()
			snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
				Table: readTestTableResource(table), Format: format,
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := snapshotPort.(*duckDBReadSnapshot)
			defer closeReadSnapshot(t, snapshot)
			iterator, err := snapshot.OpenRange(ctx, 0, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := iterator.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if batch.RowCount != 1 || len(batch.SerializedRows) == 0 {
				t.Fatalf("%s batch rows/bytes = %d/%d", format, batch.RowCount, len(batch.SerializedRows))
			}
			switch format {
			case readdomain.FormatArrow:
				assertSingleArrowIPCMessage(t, batch.SerializedRows, ipc.MessageRecordBatch)
			case readdomain.FormatAvro:
				if bytes.HasPrefix(batch.SerializedRows, []byte{'O', 'b', 'j', 1}) {
					t.Fatal("Avro batch contains an object-container header")
				}
			}
			if err := iterator.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDuckDBReadSnapshotPreservesExactTopLevelJSONNumberText(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "json_precision", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "document", Type: "JSON"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	want := `{"number":12345678901234567890123456789012345678}`
	insertReadTestRows(t, ctx, warehouse, table, "('"+want+"')")
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))
	snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatAvro,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotPort.(*duckDBReadSnapshot)
	defer closeReadSnapshot(t, snapshot)
	if len(snapshot.memoryRows) != 1 {
		t.Fatalf("in-memory rows = %d, want 1", len(snapshot.memoryRows))
	}
	row, err := decodeSnapshotRow(snapshot.memoryRows[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 1 || row[0].Text != want {
		t.Fatalf("JSON snapshot text = %q, want %q", row[0].Text, want)
	}
}

func TestAvroDecimalUsesMinimalSignedTwosComplement(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	tests := []struct {
		input string
		scale int64
		want  []byte
	}{
		{input: "0", scale: 0, want: []byte{0}},
		{input: "127", scale: 0, want: []byte{0x7f}},
		{input: "128", scale: 0, want: []byte{0, 0x80}},
		{input: "-1", scale: 0, want: []byte{0xff}},
		{input: "-128", scale: 0, want: []byte{0x80}},
		{input: "-129", scale: 0, want: []byte{0xff, 0x7f}},
	}
	for _, testCase := range tests {
		got, err := avroDecimalBytes(testCase.input, testCase.scale)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, testCase.want) {
			t.Errorf("decimal %s = %x, want %x", testCase.input, got, testCase.want)
		}
	}
	if _, err := avroDecimalBytes("1.001", 2); err == nil {
		t.Fatal("expected a value exceeding the declared scale to fail")
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestRowRestrictionParserParameterizesDocumentedSubset(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	schema := []catalogdomain.Field{
		{Name: "id", Type: "INT64"},
		{Name: "active", Type: "BOOL"},
		{Name: "name", Type: "STRING"},
		{Name: "profile", Type: "RECORD", Fields: []catalogdomain.Field{{Name: "rank", Type: "FLOAT64"}}},
	}
	sql, args, shape, err := compileRowRestrictionWithShape("NOT (id < 2 OR profile.rank >= 3.5) AND active = TRUE", schema)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "2") || strings.Contains(sql, "3.5") || strings.Contains(sql, "TRUE") {
		t.Fatalf("restriction literals leaked into SQL: %s", sql)
	}
	if strings.Count(sql, "?") != 3 || len(args) != 3 {
		t.Fatalf("parameterized restriction = %s args=%v", sql, args)
	}
	if shape.PredicateCount != 3 || shape.LogicalOperatorCount != 3 {
		t.Fatalf("restriction shape = %#v, want 3 predicates and 3 logical operators", shape)
	}
	wantArgs := []any{int64(2), 3.5, true}
	for index := range wantArgs {
		if args[index] != wantArgs[index] {
			t.Errorf("arg %d = %#v, want %#v", index, args[index], wantArgs[index])
		}
	}
	for _, supported := range []string{"id IN (1, 2)", "id BETWEEN 1 AND 2", "id = -1", "DATE '2026-08-09' < DATE '2026-08-10'", "TIMESTAMP '2026-08-09 00:00:00+00' < TIMESTAMP '2026-08-10 00:00:00+00'"} {
		sql, args, err := compileRowRestriction(supported, schema)
		if err != nil {
			t.Errorf("supported restriction %q: %v", supported, err)
			continue
		}
		if strings.Contains(sql, "2026") || len(args) == 0 {
			t.Errorf("restriction %q was not parameterized: sql=%q args=%v", supported, sql, args)
		}
	}
	for _, supported := range []string{"CAST(id AS STRING) = '1'", "LOWER(name) = 'alice'", "STARTS_WITH(LOWER(name), 'a')"} {
		sql, args, err := compileRowRestriction(supported, schema)
		if err != nil {
			t.Errorf("supported restriction %q: %v", supported, err)
			continue
		}
		if strings.Contains(sql, "ALICE") || strings.Contains(sql, "'a'") || len(args) == 0 {
			t.Errorf("restriction %q was not parameterized: sql=%q args=%v", supported, sql, args)
		}
	}
	for _, unsupported := range []string{"FORMAT('%d', id) = '1'", "id = 1; SELECT 1"} {
		if _, _, err := compileRowRestriction(unsupported, schema); err == nil {
			t.Errorf("unsupported restriction %q unexpectedly accepted", unsupported)
		}
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageReadGoogleSQLRestrictionFixtures(t *testing.T) {
	contents, err := os.ReadFile("testdata/storage_read_google_sql_restrictions.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		SchemaVersion int `json:"schemaVersion"`
		Cases         []struct {
			CaseID         string  `json:"caseId"`
			RowRestriction string  `json:"rowRestriction"`
			ExpectedIDs    []int64 `json:"expectedIds"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(contents, &fixtures); err != nil {
		t.Fatal(err)
	}
	if fixtures.SchemaVersion != 1 || len(fixtures.Cases) == 0 {
		t.Fatalf("invalid restriction fixture metadata: %#v", fixtures)
	}

	for _, fixture := range fixtures.Cases {
		fixture := fixture
		t.Run(fixture.CaseID, func(t *testing.T) {
			ctx, cancel := duckDBReadTestContext(t)
			defer cancel()
			table := catalogdomain.Table{
				ProjectID: "fixture-project", DatasetID: "fixture_dataset", ID: "events", Type: "TABLE",
				Schema: []catalogdomain.Field{
					{Name: "id", Type: "INT64", Mode: "REQUIRED"},
					{Name: "name", Type: "STRING"},
					{Name: "active", Type: "BOOL"},
					{Name: "profile", Type: "RECORD", Fields: []catalogdomain.Field{{Name: "rank", Type: "FLOAT64"}}},
					{Name: "created_at", Type: "TIMESTAMP"},
				},
			}
			warehouse := newReadTestWarehouse(t, ctx, table)
			insertReadTestRows(t, ctx, warehouse, table, "(1, 'skip', TRUE, {'rank':2.0}, TIMESTAMPTZ '2026-08-08 00:00:00+00'), (2, 'two', TRUE, {'rank':3.0}, TIMESTAMPTZ '2026-08-08 00:00:00+00'), (3, 'three', FALSE, {'rank':3.0}, TIMESTAMPTZ '2026-08-09 00:00:00+00'), (4, 'FOUR', TRUE, {'rank':4.0}, TIMESTAMPTZ '2026-08-10 00:00:00+00')")
			materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))
			snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
				Table: readTestTableResource(table), Format: readdomain.FormatArrow,
				SelectedFields: []string{"id", "name"}, RowRestriction: fixture.RowRestriction,
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := snapshotPort.(*duckDBReadSnapshot)
			defer closeReadSnapshot(t, snapshot)
			iterator, err := snapshot.OpenRange(ctx, 0, snapshot.Metadata().RowCount, 128)
			if err != nil {
				t.Fatal(err)
			}
			var batches [][]byte
			for {
				batch, err := iterator.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				batches = append(batches, batch.SerializedRows)
			}
			if err := iterator.Close(); err != nil {
				t.Fatal(err)
			}
			ids, _ := decodeProjectedArrowRows(t, snapshot.Metadata().Schema.Serialized, batches)
			if !slices.Equal(ids, fixture.ExpectedIDs) {
				t.Fatalf("fixture %q ids=%v want=%v", fixture.CaseID, ids, fixture.ExpectedIDs)
			}
		})
	}
}

func readContractFields() []catalogdomain.Field {
	return []catalogdomain.Field{
		{Name: "bool_value", Type: "BOOL", Mode: "REQUIRED", Description: "documented flag"},
		{Name: "int_value", Type: "INT64"},
		{Name: "float_value", Type: "FLOAT64"},
		{Name: "numeric_value", Type: "NUMERIC"},
		{Name: "bignumeric_value", Type: "BIGNUMERIC"},
		{Name: "string_value", Type: "STRING"},
		{Name: "bytes_value", Type: "BYTES"},
		{Name: "date_value", Type: "DATE"},
		{Name: "datetime_value", Type: "DATETIME"},
		{Name: "time_value", Type: "TIME"},
		{Name: "timestamp_value", Type: "TIMESTAMP"},
		{Name: "json_value", Type: "JSON"},
		{Name: "record_value", Type: "RECORD", Fields: []catalogdomain.Field{{Name: "child", Type: "STRING"}}},
		{Name: "repeated_value", Type: "INT64", Mode: "REPEATED"},
	}
}
