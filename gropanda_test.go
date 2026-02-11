package gropanda

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestNew(t *testing.T) {
	tc := suite.Case(t)
	require.NotNil(t, tc)
	require.NotNil(t, tc.Deps.Config)

	assert.NotEmpty(t, tc.Deps.Config.Prefix)
	assert.NotEmpty(t, tc.Deps.Config.SchemaRegistryAddress)
	assert.NotEmpty(t, tc.Deps.Config.AdminProxyAddress)
	assert.NotEmpty(t, tc.Deps.Config.HTTPProxyAddress)
	assert.NotEmpty(t, tc.Deps.Config.Brokers)
}

func UnexpectedError() error {
	return errors.New(uuid.NewString())
}

func TestRunContainer(t *testing.T) {
	t.Run("should be able broken", func(t *testing.T) {
		t.Run("when can't run panda container", func(t *testing.T) {
			exp := UnexpectedError()
			_, err := runContainer(t.Context(), Settings{}, uuid.NewString(), config{
				runner: func(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (PandaContainer, error) {
					return nil, exp
				},
			})
			require.ErrorIs(t, err, exp)
		})

		t.Run("when can't get kafka seed broker", func(t *testing.T) {
			exp := UnexpectedError()
			cont := NewMockPandaContainer(t)
			cont.EXPECT().KafkaSeedBroker(t.Context()).Return("", exp)
			cont.EXPECT().Terminate(mock.Anything).Return(nil).Maybe()

			_, err := runContainer(t.Context(), Settings{}, uuid.NewString(), config{
				runner: func(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (PandaContainer, error) {
					return cont, nil
				},
			})
			require.ErrorIs(t, err, exp)
		})

		t.Run("when can't get http proxy address", func(t *testing.T) {
			exp := UnexpectedError()
			cont := NewMockPandaContainer(t)
			cont.EXPECT().KafkaSeedBroker(t.Context()).Return("", nil)
			cont.EXPECT().HTTPProxyAddress(t.Context()).Return("", exp)
			cont.EXPECT().Terminate(mock.Anything).Return(nil).Maybe()

			_, err := runContainer(t.Context(), Settings{}, uuid.NewString(), config{
				runner: func(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (PandaContainer, error) {
					return cont, nil
				},
			})
			require.ErrorIs(t, err, exp)
		})

		t.Run("when can't get admin api address", func(t *testing.T) {
			exp := UnexpectedError()
			cont := NewMockPandaContainer(t)
			cont.EXPECT().KafkaSeedBroker(t.Context()).Return("", nil)
			cont.EXPECT().HTTPProxyAddress(t.Context()).Return("", nil)
			cont.EXPECT().AdminAPIAddress(t.Context()).Return("", exp)
			cont.EXPECT().Terminate(mock.Anything).Return(nil).Maybe()

			_, err := runContainer(t.Context(), Settings{}, uuid.NewString(), config{
				runner: func(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (PandaContainer, error) {
					return cont, nil
				},
			})
			require.ErrorIs(t, err, exp)
		})

		t.Run("when can't get schema registry address", func(t *testing.T) {
			exp := UnexpectedError()
			cont := NewMockPandaContainer(t)
			cont.EXPECT().KafkaSeedBroker(t.Context()).Return("", nil)
			cont.EXPECT().HTTPProxyAddress(t.Context()).Return("", nil)
			cont.EXPECT().AdminAPIAddress(t.Context()).Return("", nil)
			cont.EXPECT().SchemaRegistryAddress(t.Context()).Return("", exp)
			cont.EXPECT().Terminate(mock.Anything).Return(nil).Maybe()

			_, err := runContainer(t.Context(), Settings{}, uuid.NewString(), config{
				runner: func(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (PandaContainer, error) {
					return cont, nil
				},
			})
			require.ErrorIs(t, err, exp)
		})
	})
}

func TestBootstrapper(t *testing.T) {
	exp := UnexpectedError()
	cont := NewMockPandaContainer(t)
	cont.EXPECT().KafkaSeedBroker(t.Context()).Return("", nil)
	cont.EXPECT().HTTPProxyAddress(t.Context()).Return("", exp)
	cont.EXPECT().Terminate(mock.Anything).Return(nil).Maybe()

	_, err := bootstrapper[Deps](config{
		runner: func(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (PandaContainer, error) {
			return cont, nil
		},
	})(t.Context())
	require.ErrorIs(t, err, exp)
}
