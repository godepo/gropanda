// Package gropanda provides a Redpanda integration for groat-based tests.
// It leverages testcontainers-go to spin up Redpanda instances for integration testing.
//
//go:generate go tool mockery
package gropanda

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/godepo/groat/integration"
	"github.com/godepo/groat/pkg/ctxgroup"

	"github.com/testcontainers/testcontainers-go"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/godepo/gropanda/internal/pkg/containersync"
)

type (
	// PandaContainer defines the interface for interacting with a Redpanda container.
	PandaContainer interface {
		// KafkaSeedBroker returns the Kafka seed broker address.
		KafkaSeedBroker(ctx context.Context) (string, error)
		// AdminAPIAddress returns the Redpanda Admin API address.
		AdminAPIAddress(ctx context.Context) (string, error)
		// SchemaRegistryAddress returns the Redpanda Schema Registry address.
		SchemaRegistryAddress(ctx context.Context) (string, error)
		// HTTPProxyAddress returns the Redpanda HTTP Proxy address.
		HTTPProxyAddress(ctx context.Context) (string, error)

		// Terminate shuts down the container.
		Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
	}

	containerRunner func(
		ctx context.Context,
		img string,
		opts ...testcontainers.ContainerCustomizer,
	) (PandaContainer, error)

	// Container represents a Redpanda integration container for testing.
	// It provides Redpanda settings and manages its lifecycle during tests.
	Container[T any] struct {
		forks       *atomic.Int32
		ctx         context.Context
		injectLabel string
		settings    Settings
		nameSpace   string
	}

	// Settings contains Redpanda connection information and configuration.
	Settings struct {
		// Brokers is a list of Kafka broker addresses.
		Brokers []string
		// Prefix is an optional prefix for resources created in Redpanda (e.g. topics).
		Prefix string
		// HTTPProxyAddress is the address of the Redpanda HTTP Proxy.
		HTTPProxyAddress string
		// AdminProxyAddress is the address of the Redpanda Admin API.
		AdminProxyAddress string
		// SchemaRegistryAddress is the address of the Redpanda Schema Registry.
		SchemaRegistryAddress string
	}

	config struct {
		imageEnvValue  string
		containerImage string
		runner         containerRunner
		injectLabel    string
		opts           []testcontainers.ContainerCustomizer
		nameSpace      string
	}

	// Option represents a configuration option for configuring the Redpanda container construction.
	// Used to customize container behavior and specify dependency injection.
	Option func(*config)
)

// WithEnableSASL enables SASL authentication for the Redpanda container.
func WithEnableSASL(cfg *config) {
	cfg.opts = append(cfg.opts, redpanda.WithEnableSASL())
}

// WithEnableKafkaAuthorization enables Kafka authorization for the Redpanda container.
func WithEnableKafkaAuthorization(cfg *config) {
	cfg.opts = append(cfg.opts, redpanda.WithEnableKafkaAuthorization())
}

// WithNewServiceAccount adds a new service account to the Redpanda container.
func WithNewServiceAccount(user, pass string) Option {
	return func(c *config) {
		c.opts = append(c.opts, redpanda.WithNewServiceAccount(user, pass))
	}
}

// WithSuperusers adds superusers to the Redpanda container.
func WithSuperusers(users ...string) Option {
	return func(c *config) {
		c.opts = append(c.opts, redpanda.WithSuperusers(users...))
	}
}

// WithEnableSchemaRegistryHTTPBasicAuth enables HTTP Basic Auth for the Schema Registry.
func WithEnableSchemaRegistryHTTPBasicAuth(c *config) {
	c.opts = append(c.opts, redpanda.WithEnableSchemaRegistryHTTPBasicAuth())
}

// WithNameSpace sets a namespace for the Redpanda integration.
func WithNameSpace(ns string) Option {
	return func(c *config) {
		c.nameSpace = ns
	}
}

// WithInjectLabel sets the injection label for injection settings information in dependency structure by
// tag. For example when label is 'redpanda':
//
//	type Deps struct {
//	     Config *gropanda.Settings `groat:"redpanda"`
//	}
func WithInjectLabel(label string) Option {
	return func(c *config) {
		c.injectLabel = label
	}
}

// New creates a new Redpanda integration bootstrapper.
func New[T any](options ...Option) integration.Bootstrap[T] {
	cfg := config{
		imageEnvValue:  "GROAT_I9N_REDPANDA_IMAGE",
		containerImage: "docker.redpanda.com/redpandadata/redpanda:v25.3.6",
		injectLabel:    "redpanda",
	}
	cfg.runner = func(
		ctx context.Context,
		img string,
		opts ...testcontainers.ContainerCustomizer,
	) (PandaContainer, error) {
		return redpanda.Run(ctx, img, cfg.opts...)
	}

	for _, op := range options {
		op(&cfg)
	}

	if env := os.Getenv(cfg.imageEnvValue); env != "" {
		cfg.containerImage = env
	}

	return bootstrapper[T](cfg)
}

func bootstrapper[T any](cfg config) integration.Bootstrap[T] {
	return func(ctx context.Context) (integration.Injector[T], error) {
		settings := Settings{
			HTTPProxyAddress:      os.Getenv("GROAT_I9N_REDPANDA_HTTP_PROXY_ADDRESS"),
			AdminProxyAddress:     os.Getenv("GROAT_I9N_REDPANDA_ADMIN_PROXY_ADDRESS"),
			SchemaRegistryAddress: os.Getenv("GROAT_I9N_REDPANDA_SCHEMA_REGISTRY_ADDRESS"),
		}

		brokers := os.Getenv("GROAT_I9N_REDPANDA_BROKERS")

		if brokers == "" {
			newSettings, err := runContainer(ctx, settings, brokers, cfg)
			if err != nil {
				return nil, err
			}

			settings = newSettings
		}

		container := newContainer[T](ctx, settings, cfg)

		return container.Injector, nil
	}
}

func runContainer(ctx context.Context, settings Settings, brokers string, cfg config) (Settings, error) {
	settings.Brokers = strings.Split(brokers, ",")

	redpandaContainer, err := cfg.runner(ctx, cfg.containerImage)
	if err != nil {
		return settings, fmt.Errorf("redpanda container failed to run: %w", err)
	}

	ctxgroup.IncAt(ctx)

	go containersync.Terminator(ctx, redpandaContainer.Terminate)()

	connectionString, err := redpandaContainer.KafkaSeedBroker(ctx)
	if err != nil {
		return settings, fmt.Errorf("can't get connection string: %w", err)
	}

	settings.Brokers = []string{connectionString}

	httpProxy, err := redpandaContainer.HTTPProxyAddress(ctx)
	if err != nil {
		return settings, fmt.Errorf("can't get http proxy address: %w", err)
	}

	settings.HTTPProxyAddress = httpProxy

	adminProxy, err := redpandaContainer.AdminAPIAddress(ctx)
	if err != nil {
		return settings, fmt.Errorf("can't get admin proxy address: %w", err)
	}

	settings.AdminProxyAddress = adminProxy

	schemaRegistry, err := redpandaContainer.SchemaRegistryAddress(ctx)
	if err != nil {
		return settings, fmt.Errorf("can't get schema registry address: %w", err)
	}

	settings.SchemaRegistryAddress = schemaRegistry

	return settings, nil
}
