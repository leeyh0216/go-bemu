package application

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	"github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type testClock struct{ value time.Time }

func (c testClock) Now() time.Time { return c.value }

type testIDs struct{}

func (testIDs) NewID() string { return "generated" }

type testObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	opens   int
}

func (s *testObjectStore) Get(_ context.Context, uri string) (ports.ObjectInfo, error) {
	payload, ok := s.objects[uri]
	if !ok {
		return ports.ObjectInfo{}, domain.ErrNotFound
	}
	return ports.ObjectInfo{URI: uri, Size: int64(len(payload))}, nil
}
func (s *testObjectStore) List(_ context.Context, pattern string) ([]ports.ObjectInfo, error) {
	result := make([]ports.ObjectInfo, 0)
	for uri, payload := range s.objects {
		matched, _ := filepath.Match(pattern, uri)
		if matched {
			result = append(result, ports.ObjectInfo{URI: uri, Size: int64(len(payload))})
		}
	}
	return result, nil
}
func (s *testObjectStore) Open(_ context.Context, object ports.ObjectInfo) (io.ReadCloser, error) {
	s.mu.Lock()
	s.opens++
	s.mu.Unlock()
	return io.NopCloser(bytes.NewReader(s.objects[object.URI])), nil
}

type testCatalog struct{ table domain.Table }

func (c testCatalog) GetTable(_ context.Context, reference domain.TableReference) (domain.Table, error) {
	if reference != c.table.Reference {
		return domain.Table{}, domain.ErrNotFound
	}
	return c.table, nil
}

type testLoader struct {
	mu    sync.Mutex
	calls int
	paths []string
	block bool
}

func (l *testLoader) Load(ctx context.Context, request ports.LoadRequest) (ports.LoadResult, error) {
	l.mu.Lock()
	l.calls++
	for _, object := range request.Objects {
		l.paths = append(l.paths, object.Path)
		if _, err := os.Stat(object.Path); err != nil {
			l.mu.Unlock()
			return ports.LoadResult{}, err
		}
	}
	l.mu.Unlock()
	if l.block {
		<-ctx.Done()
		return ports.LoadResult{}, ctx.Err()
	}
	return ports.LoadResult{OutputRows: 3}, nil
}

func TestServiceIsIdempotentAndCleansWorkspace(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"file:///source.parquet": []byte("parquet")}}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-1"}
	configuration := testConfiguration(domain.FormatParquet)
	first, err := service.Submit(context.Background(), reference, configuration)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Submit(context.Background(), reference, configuration)
	if err != nil {
		t.Fatal(err)
	}
	if first.Reference != second.Reference {
		t.Fatalf("idempotent references differ")
	}
	job := waitForDone(t, service, reference)
	if job.Error != nil || job.Statistics.InputFiles != 1 || job.Statistics.InputBytes != 7 || job.Statistics.OutputRows != 3 {
		t.Fatalf("job = %+v", job)
	}
	loader.mu.Lock()
	calls, paths := loader.calls, append([]string(nil), loader.paths...)
	loader.mu.Unlock()
	if calls != 1 {
		t.Fatalf("loader calls = %d", calls)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("workspace file remains: %s (%v)", path, err)
		}
	}
	configuration.SourceURIs = []string{"file:///different.parquet"}
	if _, err := service.Submit(context.Background(), reference, configuration); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("configuration conflict = %v", err)
	}
}

func TestServiceConcurrentIdempotentSubmissionsExecuteOnce(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"file:///source.parquet": []byte("parquet")}}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-concurrent"}
	start := make(chan struct{})
	errorsChannel := make(chan error, 32)
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, err := service.Submit(context.Background(), reference, testConfiguration(domain.FormatParquet))
			errorsChannel <- err
		}()
	}
	close(start)
	group.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = waitForDone(t, service, reference)
	loader.mu.Lock()
	calls := loader.calls
	loader.mu.Unlock()
	if calls != 1 {
		t.Fatalf("loader calls = %d", calls)
	}
}

func TestServiceRecordsStrictFormatGapWithoutDownloading(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"file:///source.avro": []byte("secret")}}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-avro"}
	configuration := testConfiguration(domain.FormatAvro)
	configuration.SourceURIs = []string{"file:///source.avro"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "notImplemented" {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	if opens != 0 || loader.calls != 0 {
		t.Fatalf("unsupported format performed IO: opens=%d loads=%d", opens, loader.calls)
	}
}

func TestServicePersistsTimeoutAsTerminalError(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"file:///source.parquet": []byte("parquet")}}
	loader := &testLoader{block: true}
	service := newTestService(t, objects, loader, 10*time.Millisecond)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-timeout"}
	if _, err := service.Submit(context.Background(), reference, testConfiguration(domain.FormatParquet)); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "backendError" {
		t.Fatalf("job = %+v", job)
	}
}

func newTestService(t *testing.T, objects ports.ObjectStore, loader ports.Loader, timeout time.Duration) *Service {
	t.Helper()
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"}, Location: "US",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	config.OperationTimeout = timeout
	config.MaxObjectBytes = 1024
	config.MaxTotalBytes = 2048
	service, err := NewService(NewMemoryJobRepository(), objects, testCatalog{table: table}, loader, testClock{value: time.Unix(1, 0)}, testIDs{}, config)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func testConfiguration(format domain.SourceFormat) domain.LoadConfiguration {
	return domain.LoadConfiguration{
		SourceURIs: []string{"file:///source.parquet"}, Destination: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		SourceFormat: format, WriteDisposition: domain.WriteAppend,
	}
}

func waitForDone(t *testing.T, service *Service, reference domain.JobReference) *domain.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := service.Get(context.Background(), reference)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == domain.JobDone {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("load job did not finish")
	return nil
}
