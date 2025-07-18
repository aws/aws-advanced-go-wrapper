package otlp_test

import (
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/otlp"
	"github.com/stretchr/testify/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestNewOpenTelemetryGauge_Success(t *testing.T) {
	provider := sdkmetric.NewMeterProvider()
	meter := provider.Meter("test-meter")

	gauge, err := otlp.NewOpenTelemetryGauge("my_gauge", meter)

	assert.NoError(t, err)
	assert.NotNil(t, gauge)
}
