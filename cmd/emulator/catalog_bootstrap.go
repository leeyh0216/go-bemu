package main

import (
	"context"
	"fmt"
	"maps"

	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func bootstrapCatalog(
	ctx context.Context,
	cfg config.Config,
	catalog application.CatalogBootstrapCatalog,
) error {
	descriptors := bootstrapCatalogDescriptors(cfg)
	plan, err := application.NewCatalogBootstrapPlan(descriptors)
	if err != nil {
		return fmt.Errorf("prepare catalog bootstrap: %w", err)
	}
	bootstrapper, err := application.NewCatalogBootstrapper(catalog)
	if err != nil {
		return fmt.Errorf("configure catalog bootstrap: %w", err)
	}
	if err := bootstrapper.Apply(ctx, plan); err != nil {
		return fmt.Errorf("apply catalog bootstrap: %w", err)
	}
	return nil
}

func bootstrapCatalogDescriptors(cfg config.Config) []application.CatalogBootstrapProjectDescriptor {
	descriptors := make([]application.CatalogBootstrapProjectDescriptor, 0, len(cfg.Bootstrap.Projects)+1)
	defaultDeclared := false
	for _, configuredProject := range cfg.Bootstrap.Projects {
		if configuredProject.ID == cfg.Defaults.ProjectID {
			defaultDeclared = true
		}
		datasets := make([]domain.Dataset, len(configuredProject.Datasets))
		for index, configuredDataset := range configuredProject.Datasets {
			location := configuredDataset.Location
			if location == "" {
				location = cfg.Defaults.Location
			}
			datasets[index] = domain.Dataset{
				ProjectID: configuredProject.ID, ID: configuredDataset.ID,
				FriendlyName: configuredDataset.FriendlyName, Description: configuredDataset.Description,
				Location: location, Labels: maps.Clone(configuredDataset.Labels),
				DefaultTableExpirationMs:     domain.CloneOptionalInt64(configuredDataset.DefaultTableExpirationMs),
				DefaultPartitionExpirationMs: domain.CloneOptionalInt64(configuredDataset.DefaultPartitionExpirationMs),
			}
		}
		descriptors = append(descriptors, application.CatalogBootstrapProjectDescriptor{
			Project: domain.Project{
				ID: configuredProject.ID, FriendlyName: configuredProject.FriendlyName,
				Description: configuredProject.Description,
			},
			Datasets: datasets,
		})
	}
	if !defaultDeclared {
		descriptors = append([]application.CatalogBootstrapProjectDescriptor{{
			Project: domain.Project{ID: cfg.Defaults.ProjectID, FriendlyName: "BQEMU default project"},
		}}, descriptors...)
	}
	return descriptors
}
