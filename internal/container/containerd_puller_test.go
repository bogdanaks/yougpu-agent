package container

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/transfer"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestNormalizeRefMatchesDockerNaming(t *testing.T) {
	cases := map[string]string{
		"ubuntu":                                 "docker.io/library/ubuntu:latest",
		"pytorch/pytorch:2.0":                    "docker.io/pytorch/pytorch:2.0",
		"ghcr.io/yougpu/jupyter-pytorch:stage":   "ghcr.io/yougpu/jupyter-pytorch:stage",
		"quay.io/jupyter/base-notebook":          "quay.io/jupyter/base-notebook:latest",
		"registry:5000/repo:v1":                  "registry:5000/repo:v1",
		"ghcr.io/yougpu/comfyui@sha256:" + hex64: "ghcr.io/yougpu/comfyui@sha256:" + hex64,
	}
	for in, want := range cases {
		got, err := normalizeRef(in)
		if err != nil || got != want {
			t.Errorf("normalizeRef(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := normalizeRef("Bad Ref"); err == nil {
		t.Error("invalid reference must be rejected")
	}
}

const hex64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func layerEvent(event, digest string, progress, total int64) transfer.Progress {
	return transfer.Progress{
		Event:    event,
		Name:     "layer-" + digest,
		Progress: progress,
		Total:    total,
		Desc:     &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageLayerZstd, Size: total},
	}
}

func TestTransferProgressCountsBytesOfLayers(t *testing.T) {
	a := newTransferAggregator(time.Now)
	a.apply(layerEvent("waiting", "a", 0, 100))
	a.apply(layerEvent("waiting", "b", 0, 300))
	a.apply(transfer.Progress{Event: "downloading", Name: "manifest-x", Progress: 5, Total: 10,
		Desc: &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Size: 10}})
	a.apply(transfer.Progress{Event: "Pulling from ghcr.io/yougpu/x"})
	a.apply(layerEvent("downloading", "a", 100, 100))
	a.apply(layerEvent("downloading", "b", 100, 300))

	if p := a.progress(); p.Percent != 50 || p.LayersDone != 0 || p.LayersTotal != 2 {
		t.Fatalf("got %+v, want 50%% 0/2", p)
	}

	a.apply(layerEvent("complete", "a", 100, 100))
	a.apply(layerEvent("extracting", "b", 100, 300))
	if p := a.progress(); p.Percent != 100 || p.LayersDone != 0 || p.LayersTotal != 2 {
		t.Fatalf("downloaded but not unpacked: got %+v, want 100%% 0/2", p)
	}

	a.apply(layerEvent("extracted", "a", 100, 100))
	a.apply(layerEvent("extracted", "b", 300, 300))
	a.apply(layerEvent("extracted", "b", 300, 300))
	if p := a.progress(); p.Percent != 100 || p.LayersDone != 2 || p.LayersTotal != 2 {
		t.Fatalf("got %+v, want 100%% 2/2", p)
	}
}

func TestTransferProgressAlreadyExistsIsDone(t *testing.T) {
	a := newTransferAggregator(time.Now)
	a.apply(layerEvent("already exists", "a", 100, 100))
	a.apply(layerEvent("waiting", "b", 0, 100))
	if p := a.progress(); p.Percent != 50 || p.LayersDone != 1 || p.LayersTotal != 2 {
		t.Fatalf("got %+v, want 50%% 1/2", p)
	}
}

func TestTransferProgressReportsOnlyChanges(t *testing.T) {
	a := newTransferAggregator(time.Now)
	if !a.apply(layerEvent("downloading", "a", 10, 100)) {
		t.Fatal("first progress must be reported")
	}
	if a.apply(layerEvent("downloading", "a", 10, 100)) {
		t.Fatal("same progress must not be reported again")
	}
	if a.apply(transfer.Progress{Event: "saved", Name: "ghcr.io/yougpu/x:stage"}) {
		t.Fatal("events without a layer must not be reported")
	}
}

func TestTransferAggregatorTimesTheLastDownloadedLayer(t *testing.T) {
	start := time.Unix(1000, 0)
	now := start
	a := newTransferAggregator(func() time.Time { return now })
	a.apply(layerEvent("waiting", "a", 0, 100))
	a.apply(layerEvent("waiting", "b", 0, 100))
	a.apply(layerEvent("already exists", "c", 100, 100))

	now = start.Add(5 * time.Second)
	a.apply(layerEvent("extracting", "a", 100, 100))
	now = start.Add(8 * time.Second)
	a.apply(layerEvent("downloading", "b", 60, 100))
	if got := a.downloadedIn(start); got != 0 {
		t.Fatalf("layer b is still downloading, got %v", got)
	}

	now = start.Add(12 * time.Second)
	a.apply(layerEvent("complete", "b", 100, 100))
	now = start.Add(20 * time.Second)
	a.apply(layerEvent("extracted", "a", 100, 100))
	a.apply(layerEvent("extracted", "b", 100, 100))

	if got := a.downloadedIn(start); got != 12*time.Second {
		t.Fatalf("download must end with the last layer at 12s, got %v", got)
	}
	if got := a.totalBytes(); got != 300 {
		t.Fatalf("total bytes = %d, want 300", got)
	}
}

func TestUnpackSafetyRefusesOldContainerd(t *testing.T) {
	if err := checkParallelUnpack("v2.2.1"); err == nil || !strings.Contains(err.Error(), "v2.2.1") {
		t.Fatalf("containerd v2.2.1 loses deleted files when unpacking in parallel, got %v", err)
	}
	if err := checkParallelUnpack("v2.4.1"); err != nil {
		t.Fatalf("v2.4.1 is fixed, got %v", err)
	}
}

type stubPuller struct {
	err   error
	calls []string
	name  string
}

func (f *stubPuller) Pull(_ context.Context, image string, onProgress func(PullProgress)) error {
	f.calls = append(f.calls, image)
	if onProgress != nil {
		onProgress(PullProgress{Percent: 100, LayersDone: 1, LayersTotal: 1})
	}
	return f.err
}

type fakeStore struct {
	containerd bool
	err        error
}

func (f fakeStore) UsesContainerdStore(context.Context) (bool, error) { return f.containerd, f.err }

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestFallbackPullerUsesContainerdWhenDockerKeepsImagesThere(t *testing.T) {
	fast, docker := &stubPuller{}, &stubPuller{}
	p := NewFallbackPuller(fast, docker, fakeStore{containerd: true}, discard())
	if err := p.Pull(context.Background(), "ghcr.io/yougpu/x:stage", nil); err != nil {
		t.Fatal(err)
	}
	if len(fast.calls) != 1 || len(docker.calls) != 0 {
		t.Fatalf("containerd calls %v, docker calls %v", fast.calls, docker.calls)
	}
}

func TestFallbackPullerFallsBackToDockerOnError(t *testing.T) {
	fast, docker := &stubPuller{err: errors.New("transfer: connection refused")}, &stubPuller{}
	var reports []PullProgress
	p := NewFallbackPuller(fast, docker, fakeStore{containerd: true}, discard())
	if err := p.Pull(context.Background(), "ghcr.io/yougpu/x:stage", func(pp PullProgress) { reports = append(reports, pp) }); err != nil {
		t.Fatal(err)
	}
	if len(fast.calls) != 1 || len(docker.calls) != 1 {
		t.Fatalf("containerd calls %v, docker calls %v", fast.calls, docker.calls)
	}
	if len(reports) == 0 {
		t.Fatal("docker progress must still reach the caller")
	}
}

func TestFallbackPullerSkipsContainerdWhenDockerUsesItsOwnStore(t *testing.T) {
	for _, store := range []fakeStore{{containerd: false}, {err: errors.New("docker info: timeout")}} {
		fast, docker := &stubPuller{}, &stubPuller{}
		p := NewFallbackPuller(fast, docker, store, discard())
		if err := p.Pull(context.Background(), "ubuntu", nil); err != nil {
			t.Fatal(err)
		}
		if len(fast.calls) != 0 || len(docker.calls) != 1 {
			t.Fatalf("store %+v: containerd calls %v, docker calls %v", store, fast.calls, docker.calls)
		}
	}
}

func TestFallbackPullerReturnsDockerError(t *testing.T) {
	fast, docker := &stubPuller{err: errors.New("fast failed")}, &stubPuller{err: errors.New("manifest unknown")}
	p := NewFallbackPuller(fast, docker, fakeStore{containerd: true}, discard())
	if err := p.Pull(context.Background(), "ghcr.io/yougpu/x:missing", nil); err == nil || err.Error() != "manifest unknown" {
		t.Fatalf("got %v, want docker error", err)
	}
}
