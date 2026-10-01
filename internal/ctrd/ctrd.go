package ctrd

import (
	"context"
	"strconv"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
)

const (
	Socket             = "/run/containerd/containerd.sock"
	SafeParallelUnpack = "v2.4.1"
	probeTimeout       = 5 * time.Second
)

func ServerVersion(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	c, err := containerd.New(Socket, containerd.WithTimeout(probeTimeout))
	if err != nil {
		return "", err
	}
	defer c.Close()
	v, err := c.Version(ctx)
	if err != nil {
		return "", err
	}
	return v.Version, nil
}

func SupportsParallelUnpack(version string) bool {
	have, ok := parse(version)
	if !ok {
		return false
	}
	want, _ := parse(SafeParallelUnpack)
	for i := range want {
		if have[i] != want[i] {
			return have[i] > want[i]
		}
	}
	return true
}

func parse(version string) ([3]int, bool) {
	var out [3]int
	core, _, _ := strings.Cut(strings.TrimPrefix(version, "v"), "-")
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return out, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return out, false
		}
		out[i] = n
	}
	return out, true
}
