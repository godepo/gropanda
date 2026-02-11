# GroPanda

[![codecov](https://codecov.io/gh/godepo/gropanda/graph/badge.svg?token=PxxK11IBFs)](https://codecov.io/gh/godepo/gropanda)
[![Go Report Card](https://goreportcard.com/badge/godepo/gropanda)](https://goreportcard.com/report/godepo/gropanda)
[![License](https://img.shields.io/badge/License-MIT%202.0-blue.svg)](https://github.com/godepo/gropanda/blob/main/LICENSE)

GroPanda is a [groat](https://github.com/godepo/groat) module for Redpanda integration testing. It simplifies the process of spinning up Redpanda containers using [testcontainers-go](https://github.com/testcontainers/testcontainers-go) and injecting connection details into your tests.

## Installation

```bash
go get github.com/godepo/gropanda
```

## Usage

GroPanda is designed to be used with the `groat` testing framework. Below is an example of how to set up a test suite with Redpanda.

### Example

```go
package mytest

import (
    "os"
    "testing"

    "github.com/godepo/groat"
    "github.com/godepo/groat/integration"
    "github.com/godepo/gropanda"
)

type (
    // Deps defines the dependencies for your tests.
    // The `groat:"redpanda"` tag tells groat to inject Redpanda settings here.
    Deps struct {
        Config *gropanda.Settings `groat:"redpanda"`
    }

    State struct {
        // Your test state here
    }

    SystemUnderTest struct {
        // Your system under test here
    }
)

var suite *integration.Container[Deps, State, *SystemUnderTest]

func TestMain(m *testing.M) {
    suite = integration.New[Deps, State, *SystemUnderTest](
        m,
        func(t *testing.T) *groat.Case[Deps, State, *SystemUnderTest] {
            return groat.New[Deps, State, *SystemUnderTest](t, func(t *testing.T, deps Deps) *SystemUnderTest {
                // Initialize your system under test using deps.Config
                return &SystemUnderTest{}
            })
        },
        gropanda.New[Deps](
            gropanda.WithInjectLabel("redpanda"),
            gropanda.WithEnableSASL,
            gropanda.WithNewServiceAccount("user", "pass"),
        ),
    )
    os.Exit(suite.Go())
}

func TestExample(t *testing.T) {
    tc := suite.Case(t)
    // Use tc.Deps.Config to interact with Redpanda
    if len(tc.Deps.Config.Brokers) == 0 {
        t.Fatal("Expected Redpanda brokers to be configured")
    }
}
```

## Configuration

GroPanda can be configured using `Option` functions passed to `gropanda.New`:

- `WithInjectLabel(label string)`: Sets the tag used for dependency injection (default: `"redpanda"`).
- `WithNameSpace(ns string)`: Sets a prefix for the `Settings.Prefix` field.
- `WithEnableSASL`: Enables SASL authentication.
- `WithEnableKafkaAuthorization`: Enables Kafka authorization.
- `WithNewServiceAccount(user, pass string)`: Adds a service account.
- `WithSuperusers(users ...string)`: Configures superusers.
- `WithEnableSchemaRegistryHTTPBasicAuth`: Enables Basic Auth for Schema Registry.

### Environment Variables

You can override some defaults using environment variables:

- `GROAT_I9N_REDPANDA_IMAGE`: Override the default Redpanda Docker image.
- `GROAT_I9N_REDPANDA_BROKERS`: If set, GroPanda will use these brokers instead of starting a container.
- `GROAT_I9N_REDPANDA_HTTP_PROXY_ADDRESS`
- `GROAT_I9N_REDPANDA_ADMIN_PROXY_ADDRESS`
- `GROAT_I9N_REDPANDA_SCHEMA_REGISTRY_ADDRESS`

## License

MIT
