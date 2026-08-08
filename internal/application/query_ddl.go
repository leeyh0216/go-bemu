package application

import "github.com/leeyh0216/go-bemu/internal/ports"

// WithQueryDDLExecutor installs the catalog-owned semantic DDL use case.
func WithQueryDDLExecutor(executor ports.DDLExecutor) QueryOption {
	return func(service *QueryService) { service.ddlExecutor = executor }
}
