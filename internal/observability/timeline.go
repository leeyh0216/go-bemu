package observability

import (
	"encoding/base64"
	"encoding/json"
	"sync"
	"time"
)

var processTimeline = struct {
	sync.RWMutex
	timeline *Timeline
}{timeline: NewTimeline(TimelineConfig{})}

// ConfigureTimeline replaces the process recorder at startup. Existing
// snapshots stay valid because callers only receive copied event values.
func ConfigureTimeline(config TimelineConfig) *Timeline {
	timeline := NewTimeline(config)
	processTimeline.Lock()
	processTimeline.timeline = timeline
	processTimeline.Unlock()
	return timeline
}

func ProcessTimeline() *Timeline {
	processTimeline.RLock()
	defer processTimeline.RUnlock()
	return processTimeline.timeline
}

// TimelineConfig bounds the diagnostic data retained by a process. The limits
// apply after payload truncation, so recording never waits for a consumer.
type TimelineConfig struct {
	MaxEvents       int
	MaxBytes        int64
	MaxPayloadBytes int64
}

func (c TimelineConfig) normalized() TimelineConfig {
	if c.MaxEvents <= 0 {
		c.MaxEvents = 5_000
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 64 << 20
	}
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = 4 << 20
	}
	if c.MaxPayloadBytes > c.MaxBytes {
		c.MaxPayloadBytes = c.MaxBytes
	}
	return c
}

// DiagnosticEvent is a transport-neutral, immutable snapshot. Payload is
// base64 rather than a string so binary HTTP and protobuf wire data retain
// their exact representation through the timeline API.
type DiagnosticEvent struct {
	Sequence       uint64              `json:"sequence"`
	WallTime       time.Time           `json:"wallTime"`
	MonotonicNanos int64               `json:"monotonicNanos"`
	RequestID      string              `json:"requestId,omitempty"`
	TraceID        string              `json:"traceId,omitempty"`
	Protocol       string              `json:"protocol"`
	OperationID    string              `json:"operationId,omitempty"`
	Phase          string              `json:"phase"`
	Method         string              `json:"method,omitempty"`
	Path           string              `json:"path,omitempty"`
	RPCMethod      string              `json:"rpcMethod,omitempty"`
	Peer           string              `json:"peer,omitempty"`
	Headers        map[string][]string `json:"headers,omitempty"`
	PayloadBase64  string              `json:"payloadBase64,omitempty"`
	PayloadJSON    string              `json:"payloadJson,omitempty"`
	OriginalBytes  int64               `json:"originalBytes"`
	Truncated      bool                `json:"truncated"`
	Status         string              `json:"status,omitempty"`
	Error          string              `json:"error,omitempty"`
	DurationNanos  int64               `json:"durationNanos,omitempty"`
}

// TimelineStats describes retained and evicted data without exposing mutable
// storage to a slow UI poller.
type TimelineStats struct {
	Events        int    `json:"events"`
	Bytes         int64  `json:"bytes"`
	DroppedEvents uint64 `json:"droppedEvents"`
	DroppedBytes  int64  `json:"droppedBytes"`
}

type TimelineSnapshot struct {
	Events []DiagnosticEvent `json:"events"`
	Stats  TimelineStats     `json:"stats"`
}

// Timeline is a concurrency-safe oldest-first ring. It copies every input on
// admission; callers may therefore reuse request buffers immediately.
type Timeline struct {
	mu            sync.RWMutex
	config        TimelineConfig
	started       time.Time
	next          uint64
	bytes         int64
	events        []storedDiagnosticEvent
	droppedEvents uint64
	droppedBytes  int64
}

type storedDiagnosticEvent struct {
	event DiagnosticEvent
	bytes int64
}

func NewTimeline(config TimelineConfig) *Timeline {
	return &Timeline{config: config.normalized(), started: time.Now()}
}

func (t *Timeline) Record(event DiagnosticEvent, payload []byte) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next++
	event.Sequence = t.next
	event.WallTime = time.Now().UTC()
	event.MonotonicNanos = time.Since(t.started).Nanoseconds()
	event.Headers = cloneHeaders(event.Headers)
	if event.OriginalBytes == 0 {
		event.OriginalBytes = int64(len(payload))
	}
	if int64(len(payload)) > t.config.MaxPayloadBytes {
		payload = payload[:t.config.MaxPayloadBytes]
		event.Truncated = true
	}
	if len(payload) > 0 {
		copied := append([]byte(nil), payload...)
		event.PayloadBase64 = base64.StdEncoding.EncodeToString(copied)
	}
	size := diagnosticEventBytes(event)
	if size > t.config.MaxBytes {
		t.droppedEvents++
		t.droppedBytes += size
		return
	}
	for len(t.events) > 0 && (len(t.events) >= t.config.MaxEvents || t.bytes+size > t.config.MaxBytes) {
		oldest := t.events[0]
		t.events[0] = storedDiagnosticEvent{}
		t.events = t.events[1:]
		t.bytes -= oldest.bytes
		t.droppedEvents++
		t.droppedBytes += oldest.bytes
	}
	t.events = append(t.events, storedDiagnosticEvent{event: event, bytes: size})
	t.bytes += size
}

func (t *Timeline) PayloadLimit() int64 {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.config.MaxPayloadBytes
}

func (t *Timeline) Snapshot(after uint64, limit int) TimelineSnapshot {
	if t == nil {
		return TimelineSnapshot{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if limit <= 0 || limit > t.config.MaxEvents {
		limit = t.config.MaxEvents
	}
	events := make([]DiagnosticEvent, 0, limit)
	for _, stored := range t.events {
		if stored.event.Sequence <= after {
			continue
		}
		events = append(events, cloneDiagnosticEvent(stored.event))
		if len(events) == limit {
			break
		}
	}
	return TimelineSnapshot{Events: events, Stats: TimelineStats{
		Events: len(t.events), Bytes: t.bytes, DroppedEvents: t.droppedEvents, DroppedBytes: t.droppedBytes,
	}}
}

func (t *Timeline) Clear() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = nil
	t.bytes = 0
}

func cloneDiagnosticEvent(event DiagnosticEvent) DiagnosticEvent {
	event.Headers = cloneHeaders(event.Headers)
	return event
}

func cloneHeaders(headers map[string][]string) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(headers))
	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

func diagnosticEventBytes(event DiagnosticEvent) int64 {
	// The API exposes JSON snapshots, so encoded bytes are the one stable,
	// transport-independent accounting unit. Go object allocation size is not a
	// portable contract and would vary by compiler/runtime.
	encoded, err := json.Marshal(event)
	if err != nil {
		return 0
	}
	return int64(len(encoded))
}
