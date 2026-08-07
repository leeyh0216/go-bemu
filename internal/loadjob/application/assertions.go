package application

import "github.com/leeyh0216/go-bemu/internal/loadjob/ports"

var _ ports.JobRepository = (*MemoryJobRepository)(nil)
