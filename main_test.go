package gropanda

import (
	"os"
	"testing"

	"github.com/godepo/groat"
	"github.com/godepo/groat/integration"
)

type (
	SystemUnderTest struct {
	}

	State struct {
	}
	Deps struct {
		Config *Settings `groat:"gropanda"`
	}
)

var suite *integration.Container[Deps, State, *SystemUnderTest]

func TestMain(m *testing.M) {
	if os.Getenv("GROAT_I9N_REDPANDA_IMAGE") == "" {
		_ = os.Setenv("GROAT_I9N_REDPANDA_IMAGE", "docker.redpanda.com/redpandadata/redpanda:v23.3.3")
	}

	suite = integration.New[Deps, State, *SystemUnderTest](
		m,
		func(t *testing.T) *groat.Case[Deps, State, *SystemUnderTest] {
			tcs := groat.New[Deps, State, *SystemUnderTest](t, func(t *testing.T, deps Deps) *SystemUnderTest {
				return &SystemUnderTest{}
			})
			return tcs
		},
		New[Deps](
			WithInjectLabel("gropanda"),
			WithNewServiceAccount("gropanda", "test"),
			WithSuperusers("gropanda"),
			WithEnableSASL,
			WithEnableKafkaAuthorization,
			WithEnableSchemaRegistryHTTPBasicAuth,
			WithNameSpace("gropanda"),
		),
	)
	os.Exit(suite.Go())
}
