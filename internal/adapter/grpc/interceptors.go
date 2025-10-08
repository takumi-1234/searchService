package grpc

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	rpcSystemGRPC = "grpc"
)

// NewUnaryMetricsInterceptor は gRPC リクエストのメトリクスを収集する UnaryInterceptor を生成します。
func NewUnaryMetricsInterceptor(meter metric.Meter) (grpc.UnaryServerInterceptor, error) {
	requestLatency, err := meter.Float64Histogram(
		"grpc.server.request.duration",
		metric.WithDescription("Latency of gRPC requests"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	requestCounter, err := meter.Int64Counter(
		"grpc.server.requests",
		metric.WithDescription("Number of gRPC requests processed"),
	)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(start).Seconds()

		service, method := splitFullMethod(info.FullMethod)
		baseAttrs := []attribute.KeyValue{
			attribute.String("rpc.system", rpcSystemGRPC),
			attribute.String("rpc.service", service),
			attribute.String("rpc.method", method),
		}

		requestLatency.Record(ctx, elapsed, metric.WithAttributes(baseAttrs...))

		code := status.Code(err)
		outcome := outcomeFromCode(code)
		counterAttrs := append(baseAttrs,
			attribute.String("grpc.status_code", code.String()),
			attribute.String("outcome", outcome),
		)
		requestCounter.Add(ctx, 1, metric.WithAttributes(counterAttrs...))

		return resp, err
	}, nil
}

func splitFullMethod(fullMethod string) (service string, method string) {
	if fullMethod == "" {
		return "unknown", "unknown"
	}

	if fullMethod[0] == '/' {
		fullMethod = fullMethod[1:]
	}

	parts := strings.Split(fullMethod, "/")
	if len(parts) != 2 {
		return fullMethod, "unknown"
	}
	return parts[0], parts[1]
}

func outcomeFromCode(code codes.Code) string {
	if code == codes.OK {
		return "success"
	}
	return "failure"
}
