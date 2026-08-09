package observability

import (
	"encoding/base64"
	"encoding/json"
	"sync"
	"testing"
)

func TestTimelineEvictsOldestEventAndCopiesInputs(t *testing.T) {
	timeline := NewTimeline(TimelineConfig{MaxEvents: 2, MaxBytes: 4 << 10, MaxPayloadBytes: 32})
	headers := map[string][]string{"X-Test": {"before"}}
	payload := []byte("first")
	timeline.Record(DiagnosticEvent{Protocol: "http", Headers: headers}, payload)
	headers["X-Test"][0] = "after"
	payload[0] = 'X'
	timeline.Record(DiagnosticEvent{Protocol: "http", OperationID: "second"}, []byte("second"))
	timeline.Record(DiagnosticEvent{Protocol: "grpc", OperationID: "third"}, []byte("third"))
	snapshot := timeline.Snapshot(0, 0)
	if len(snapshot.Events) != 2 || snapshot.Events[0].OperationID != "second" || snapshot.Stats.DroppedEvents != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	var encodedBytes int64
	for _, event := range snapshot.Events {
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		encodedBytes += int64(len(encoded))
	}
	if snapshot.Stats.Bytes != encodedBytes {
		t.Fatalf("retained accounting = %d, want %d", snapshot.Stats.Bytes, encodedBytes)
	}
	if snapshot.Events[0].PayloadBase64 != base64.StdEncoding.EncodeToString([]byte("second")) {
		t.Fatal("retained payload mismatch")
	}
}

func TestTimelineTruncatesAndRemainsRaceSafe(t *testing.T) {
	timeline := NewTimeline(TimelineConfig{MaxEvents: 100, MaxBytes: 16 << 10, MaxPayloadBytes: 3})
	timeline.Record(DiagnosticEvent{Protocol: "http"}, []byte("abcdef"))
	snapshot := timeline.Snapshot(0, 1)
	if !snapshot.Events[0].Truncated || snapshot.Events[0].OriginalBytes != 6 || snapshot.Events[0].PayloadBase64 != base64.StdEncoding.EncodeToString([]byte("abc")) {
		t.Fatalf("truncation = %#v", snapshot.Events[0])
	}
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 100; j++ {
				timeline.Record(DiagnosticEvent{Protocol: "grpc"}, []byte("payload"))
				_ = timeline.Snapshot(0, 10)
			}
		}()
	}
	group.Wait()
	if snapshot := timeline.Snapshot(0, 0); snapshot.Stats.Bytes > 16<<10 {
		t.Fatalf("retained bytes = %d", snapshot.Stats.Bytes)
	}
}
