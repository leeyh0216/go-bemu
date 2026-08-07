package rest

// Official method and paging contract:
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const gapTableDataProjectionV1 = "tabledata.selected-fields-v1"

type TableDataUseCases interface {
	ListTableData(context.Context, string, string, string, int64, int) (ports.TableDataPage, error)
}

// WithTableDataAPI explicitly composes the optional REST data browser. Metadata
// servers can omit the option without advertising a route backed by a nil row
// adapter.
func WithTableDataAPI(useCases TableDataUseCases) Option {
	return func(server *Server) {
		server.routeExtensions = append(server.routeExtensions, func(mux *http.ServeMux) {
			mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/data", func(w http.ResponseWriter, r *http.Request) {
				listTableData(w, r, useCases)
			})
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
	limit, err := tableDataMaximum(r.URL.Query().Get("maxResults"))
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := useCases.ListTableData(r.Context(), reference.ProjectID, reference.DatasetID, reference.TableID, offset, limit)
	if err != nil {
		writeError(w, err)
		return
	}
	rows, err := tableDataRows(page.Rows, page.Schema, format)
	if err != nil {
		writeError(w, err)
		return
	}
	response := tableDataListResponse{
		Kind: "bigquery#tableDataList", TotalRows: strconv.FormatInt(page.TotalRows, 10), Rows: rows,
	}
	response.ETag = metadataETag(struct {
		Scope     string
		Offset    int64
		TotalRows int64
		Rows      []tableRow
	}{Scope: tableDataPageScope(reference), Offset: offset, TotalRows: page.TotalRows, Rows: rows})
	if err := checkIfMatch(r, response.ETag); err != nil {
		writeError(w, err)
		return
	}
	nextOffset := offset + int64(len(page.Rows))
	if len(page.Rows) > 0 && nextOffset < page.TotalRows {
		response.PageToken = encodeQueryPageToken("tabledata-list", tableDataPageScope(reference), strconv.FormatInt(nextOffset, 10))
	}
	writeJSON(w, http.StatusOK, response)
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

func tableDataMaximum(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || value > uint64(math.MaxInt) {
		return 0, fmt.Errorf("%w: maxResults must be a uint32", domain.ErrInvalid)
	}
	return int(value), nil
}

func tableDataPageScope(reference domain.TableReference) string {
	return observability.Digest([]byte(strings.Join([]string{
		reference.ProjectID, reference.DatasetID, reference.TableID,
	}, "\x00")))
}
