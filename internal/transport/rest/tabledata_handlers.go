package rest

// Official method and paging contract:
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
	tabledatabudget "github.com/leeyh0216/go-bemu/internal/tabledata"
)

const gapTableDataProjectionV1 = "tabledata.selected-fields-v1"

type TableDataUseCases interface {
	ListTableData(context.Context, string, string, string, int64, ports.TableDataMaxResults) (ports.TableDataPage, error)
}

// WithTableDataAPI explicitly composes the optional REST data browser. Metadata
// servers can omit the option without advertising a route backed by a nil row
// adapter.
func WithTableDataAPI(useCases TableDataUseCases) Option {
	return func(server *Server) {
		server.operationRoutes = append(server.operationRoutes, func() []routeBinding {
			return []routeBinding{handlerBinding("bigquery.tabledata.list", func(w http.ResponseWriter, r *http.Request) {
				listTableData(w, r, useCases)
			})}
		})
		server.discoveryExtensions = append(server.discoveryExtensions, extendTableDataDiscovery)
	}
}

func listTableData(w http.ResponseWriter, r *http.Request, useCases TableDataUseCases) {
	format, err := validateTableDataOptions(r)
	if err != nil {
		writeError(w, err)
		return
	}
	reference := domain.TableReference{
		ProjectID: r.PathValue("projectId"), DatasetID: r.PathValue("datasetId"), TableID: r.PathValue("tableId"),
	}
	offset, err := tableDataOffset(r, reference)
	if err != nil {
		writeError(w, err)
		return
	}
	query := r.URL.Query()
	maximum, err := tableDataMaximum(query.Get("maxResults"), query.Has("maxResults"))
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := useCases.ListTableData(r.Context(), reference.ProjectID, reference.DatasetID, reference.TableID, offset, maximum)
	if err != nil {
		writeTableDataError(w, err)
		return
	}
	encoded, err := encodeTableDataListResponse(page, format, tableDataPageScope(reference), offset)
	if err != nil {
		writeTableDataError(w, err)
		return
	}
	if err := checkIfMatch(r, encoded.etag); err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.FormatInt(encoded.size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = encoded.WriteTo(w)
}

func writeTableDataError(w http.ResponseWriter, err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		writeError(w, &httpProtocolError{
			status: http.StatusServiceUnavailable, reason: "backendError",
			message: "table data operation exceeded the configured deadline", err: err,
		})
		return
	}
	if errors.Is(err, tabledatabudget.ErrRowTooLarge) || errors.Is(err, tabledatabudget.ErrResponseTooLarge) {
		writeError(w, &httpProtocolError{
			status: http.StatusForbidden, reason: "responseTooLarge",
			message: "table data response exceeds the configured byte limit", err: err,
		})
		return
	}
	writeError(w, err)
}

type tableDataFormatOptions struct {
	UseInt64Timestamp bool
}

func validateTableDataOptions(r *http.Request) (tableDataFormatOptions, error) {
	query := r.URL.Query()
	if query.Get("selectedFields") != "" {
		return tableDataFormatOptions{}, fmt.Errorf("%w: selectedFields is not implemented; capability=%s", domain.ErrInvalid, gapTableDataProjectionV1)
	}
	format := tableDataFormatOptions{}
	if raw := query.Get("formatOptions.useInt64Timestamp"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return tableDataFormatOptions{}, fmt.Errorf("%w: formatOptions.useInt64Timestamp must be boolean", domain.ErrInvalid)
		}
		format.UseInt64Timestamp = value
	}
	if query.Get("formatOptions.timestampOutputFormat") != "" {
		return tableDataFormatOptions{}, fmt.Errorf("%w: timestampOutputFormat is not implemented; capability=%s", domain.ErrInvalid, gapTableDataProjectionV1)
	}
	return format, nil
}

func tableDataOffset(r *http.Request, reference domain.TableReference) (int64, error) {
	rawToken, rawStart := r.URL.Query().Get("pageToken"), r.URL.Query().Get("startIndex")
	if rawToken != "" && rawStart != "" {
		return 0, fmt.Errorf("%w: pageToken and startIndex cannot be combined", domain.ErrInvalid)
	}
	if rawToken != "" {
		cursor, err := decodeQueryPageToken(rawToken, "tabledata-list", tableDataPageScope(reference))
		if err != nil {
			return 0, err
		}
		rawStart = cursor
	}
	if rawStart == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(rawStart, 10, 64)
	if err != nil || value > math.MaxInt64 {
		return 0, fmt.Errorf("%w: startIndex must be a uint64 supported by the local backend", domain.ErrInvalid)
	}
	return int64(value), nil
}

func tableDataMaximum(raw string, present bool) (ports.TableDataMaxResults, error) {
	if !present {
		return ports.TableDataMaxResults{}, nil
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || value > uint64(math.MaxInt) {
		return ports.TableDataMaxResults{}, fmt.Errorf("%w: maxResults must be a uint32", domain.ErrInvalid)
	}
	return ports.TableDataMaxResults{Value: int(value), Present: true}, nil
}

func tableDataPageScope(reference domain.TableReference) string {
	return observability.Digest([]byte(strings.Join([]string{
		reference.ProjectID, reference.DatasetID, reference.TableID,
	}, "\x00")))
}
