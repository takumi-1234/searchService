package observability

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config は可観測性の初期化に必要な設定値です。
type Config struct {
	ServiceName     string
	TracingEndpoint string
	TracingInsecure bool
}

// Provider はOpenTelemetryのメータ・トレーサプロバイダを保持します。
type Provider struct {
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	promExporter   *otelprom.Exporter
	registry       *prometheus.Registry
}

// Setup はOpenTelemetryのメータおよびトレーサを初期化し、グローバルプロバイダとして登録します。
func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.ServiceName == "" {
		return nil, fmt.Errorf("service name must not be empty")
	}

	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	registry := prometheus.NewRegistry()

	promExporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithResource(res),
	)

	traceExporter, err := newTraceExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	otel.SetMeterProvider(meterProvider)
	otel.SetTracerProvider(traceProvider)

	return &Provider{
		meterProvider:  meterProvider,
		tracerProvider: traceProvider,
		promExporter:   promExporter,
		registry:       registry,
	}, nil
}

// MetricsHandler はPrometheus形式でメトリクスを公開するHTTPハンドラを返します。
func (p *Provider) MetricsHandler() http.Handler {
	if p == nil {
		return http.NotFoundHandler()
	}
	if p.registry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{})
}

// Shutdown はメータープロバイダおよびトレーサープロバイダを停止します。
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}

	var errList []error
	if p.tracerProvider != nil {
		if err := p.tracerProvider.Shutdown(ctx); err != nil {
			errList = append(errList, err)
		}
	}
	if p.meterProvider != nil {
		if err := p.meterProvider.Shutdown(ctx); err != nil {
			errList = append(errList, err)
		}
	}

	return errors.Join(errList...)
}

func newTraceExporter(ctx context.Context, cfg Config) (*otlptrace.Exporter, error) {
	endpoint := strings.TrimSpace(cfg.TracingEndpoint)
	var opts []otlptracehttp.Option
	var urlPath string

	if endpoint == "" {
		endpoint = "localhost:4318"
	}

	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return nil, fmt.Errorf("invalid tracing endpoint %q: %w", endpoint, err)
		}
		if host := parsed.Host; host != "" {
			endpoint = host
		}
		if path := strings.TrimSpace(parsed.Path); path != "" && path != "/" {
			urlPath = path
		}
		if parsed.Scheme == "http" {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
	}

	if cfg.TracingInsecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	opts = append(opts, otlptracehttp.WithEndpoint(endpoint))
	if urlPath != "" {
		opts = append(opts, otlptracehttp.WithURLPath(urlPath))
	}

	return otlptracehttp.New(ctx, opts...)
}
