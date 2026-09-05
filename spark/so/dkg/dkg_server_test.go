package dkg

import (
	"fmt"
	"testing"

	pbdkg "github.com/lightsparkdev/spark/proto/dkg"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStartDkgRejectsInvalidKeyCount(t *testing.T) {
	server := NewServer(nil, nil)
	for _, count := range []int32{-1, 0, maxDkgKeyCount + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			_, err := server.StartDkg(t.Context(), &pbdkg.StartDkgRequest{Count: count})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}
