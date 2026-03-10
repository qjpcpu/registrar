package zk

import (
	"testing"
	"time"
)

func TestSelfHealRetryBackoff(t *testing.T) {
	testCases := []struct {
		retry int
		want  time.Duration
	}{
		{retry: 0, want: 1 * time.Second},
		{retry: 1, want: 2 * time.Second},
		{retry: 2, want: 4 * time.Second},
		{retry: 3, want: 8 * time.Second},
		{retry: 4, want: 16 * time.Second},
		{retry: 5, want: 30 * time.Second},
		{retry: 100, want: 30 * time.Second},
	}

	for _, tc := range testCases {
		if got := selfHealRetryBackoff(tc.retry); got != tc.want {
			t.Fatalf("retry=%d: backoff=%s, want=%s", tc.retry, got, tc.want)
		}
	}
}
