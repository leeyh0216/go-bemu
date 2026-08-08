package sqltest

import (
	"encoding/json"
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadFSStrictlyLoadsAndSortsSQLCases(t *testing.T) {
	files := validCaseFiles("z-case", "ordered")
	for name, file := range validCaseFiles("a-case", "ordered") {
		files["a/"+strings.TrimPrefix(name, "z/")] = file
	}
	cases, err := LoadFS(files, ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 2 || cases[0].ID != "a-case" || cases[1].ID != "z-case" {
		t.Fatalf("cases = %#v", cases)
	}
	if value := cases[0].Dataset.Projects[0].Datasets[0].Tables[0].Rows[0][0]; value != json.Number("1") {
		t.Fatalf("JSON number = %#v (%T)", value, value)
	}
}

func TestLoadFSRejectsMalformedAmbiguousAndDuplicateCases(t *testing.T) {
	tests := map[string]struct {
		mutate func(fstest.MapFS)
		want   string
	}{
		"unknown descriptor field": {
			mutate: func(files fstest.MapFS) {
				files["z/case.json"] = mapFile(`{"schemaVersion":1,"caseId":"z-case","defaultProject":"p","defaultDataset":"d","rowOrder":"ordered","extra":true}`)
			},
			want: "unknown field",
		},
		"unknown file": {
			mutate: func(files fstest.MapFS) { files["z/notes.txt"] = mapFile("not allowed") },
			want:   "unexpected case entry",
		},
		"ambiguous ordering": {
			mutate: func(files fstest.MapFS) {
				files["z/case.json"] = mapFile(`{"schemaVersion":1,"caseId":"z-case","defaultProject":"p","defaultDataset":"d","rowOrder":"none"}`)
			},
			want: "explicit ordered or unordered",
		},
		"engine SQL": {
			mutate: func(files fstest.MapFS) { files["z/query.sql"] = mapFile("SELECT value::BIGINT FROM d.t") },
			want:   "engine-specific cast",
		},
		"target-specific SQL": {
			mutate: func(files fstest.MapFS) { files["z/query.sql"] = mapFile("SELECT '" + "b" + "q'") },
			want:   "forbidden target",
		},
		"ambiguous absent table postcondition": {
			mutate: func(files fstest.MapFS) {
				files["z/expected.json"] = mapFile(`{"kind":"rows","schema":[{"name":"id","type":"INT64","mode":"REQUIRED"}],"rows":[[1]],"tables":[{"projectId":"p","datasetId":"d","tableId":"missing","exists":false,"rowOrder":"ordered"}]}`)
			},
			want: "absent expected table",
		},
		"duplicate ID": {
			mutate: func(files fstest.MapFS) {
				for name, file := range validCaseFiles("z-case", "ordered") {
					files["duplicate/"+strings.TrimPrefix(name, "z/")] = file
				}
			},
			want: "duplicate SQL case ID",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			files := validCaseFiles("z-case", "ordered")
			test.mutate(files)
			_, err := LoadFS(files, ".")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadFS() error = %v, want %q", err, test.want)
			}
		})
	}
}

func validCaseFiles(caseID, order string) fstest.MapFS {
	return fstest.MapFS{
		"z/case.json":     mapFile(`{"schemaVersion":1,"caseId":"` + caseID + `","defaultProject":"p","defaultDataset":"d","rowOrder":"` + order + `"}`),
		"z/dataset.json":  mapFile(`{"projects":[{"projectId":"p","datasets":[{"datasetId":"d","location":"US","tables":[{"tableId":"t","schema":[{"name":"id","type":"INT64","mode":"REQUIRED"}],"rows":[[1]]}]}]}]}`),
		"z/query.sql":     mapFile("SELECT id FROM d.t ORDER BY id"),
		"z/expected.json": mapFile(`{"kind":"rows","schema":[{"name":"id","type":"INT64","mode":"REQUIRED"}],"rows":[[1]]}`),
	}
}

func mapFile(contents string) *fstest.MapFile {
	return &fstest.MapFile{Data: []byte(contents), Mode: 0o644}
}
