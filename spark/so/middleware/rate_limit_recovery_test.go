package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/lightsparkdev/spark/so/knobs"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRateLimitRecoversAfterExhaustion(t *testing.T) {
	limits := knobs.NewFixedKnobs(map[string]float64{
		knobs.KnobRateLimitLimit + "@/test.Service/Recover:ip#1s": 1,
	})
	// Use the real store: the test store refills in Get and masks this bug.
	limiter, err := NewRateLimiter(&RateLimiterConfig{}, WithKnobs(limits))
	require.NoError(t, err)
	interceptor := limiter.UnaryServerInterceptor()
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-forwarded-for", "192.0.2.1"))
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Recover"}
	calls := 0
	handler := func(context.Context, any) (any, error) {
		calls++
		return "ok", nil
	}
	request := func() error {
		_, err := interceptor(ctx, nil, info, handler)
		return err
	}

	require.NoError(t, request())
	require.Equal(t, codes.ResourceExhausted, status.Code(request()))
	require.Equal(t, 1, calls)

	time.Sleep(1100 * time.Millisecond)
	require.NoError(t, request(), "an expired empty bucket must refill")
	require.Equal(t, codes.ResourceExhausted, status.Code(request()))
	require.Equal(t, 2, calls, "refill must still enforce the configured capacity")
}
