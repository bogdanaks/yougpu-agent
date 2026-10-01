package ctrd

import "testing"

func TestSupportsParallelUnpackFromFixedRelease(t *testing.T) {
	cases := map[string]bool{
		"v2.4.1":  true,
		"2.4.1":   true,
		"v2.4.12": true,
		"v2.5.0":  true,
		"v2.10.0": true,
		"v3.0.0":  true,
		"v2.4.0":  false,
		"v2.2.1":  false,
		"1.7.27":  false,
		"v1.10.0": false,
		"":        false,
		"garbage": false,
	}
	for version, want := range cases {
		if got := SupportsParallelUnpack(version); got != want {
			t.Errorf("SupportsParallelUnpack(%q) = %v, want %v", version, got, want)
		}
	}
}
