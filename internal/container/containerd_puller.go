package container

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/transfer"
	transferimage "github.com/containerd/containerd/v2/core/transfer/image"
	"github.com/containerd/containerd/v2/core/transfer/registry"
	"github.com/containerd/platforms"
	"github.com/distribution/reference"

	"github.com/bogdanaks/yougpu-agent/internal/ctrd"
)

const (
	dockerNamespace     = "moby"
	dockerSnapshotter   = "overlayfs"
	containerdStoreType = "io.containerd.snapshotter.v1"
)

var linuxAMD64 = platforms.MustParse("linux/amd64")

func normalizeRef(image string) (string, error) {
	named, err := reference.ParseDockerRef(image)
	if err != nil {
		return "", err
	}
	return named.String(), nil
}

type ContainerdPuller struct {
	address string
	log     *slog.Logger
	now     func() time.Time
}

func NewContainerdPuller(log *slog.Logger) *ContainerdPuller {
	return &ContainerdPuller{address: ctrd.Socket, log: log, now: time.Now}
}

func seconds(d time.Duration) float64 {
	return d.Round(100 * time.Millisecond).Seconds()
}

func checkParallelUnpack(version string) error {
	if !ctrd.SupportsParallelUnpack(version) {
		return fmt.Errorf("containerd %s is older than %s and loses deleted files when unpacking layers in parallel", version, ctrd.SafeParallelUnpack)
	}
	return nil
}

func (p *ContainerdPuller) Pull(ctx context.Context, image string, onProgress func(PullProgress)) error {
	ref, err := normalizeRef(image)
	if err != nil {
		return err
	}
	client, err := containerd.New(p.address, containerd.WithDefaultNamespace(dockerNamespace))
	if err != nil {
		return fmt.Errorf("containerd: %w", err)
	}
	defer client.Close()

	version, err := client.Version(ctx)
	if err != nil {
		return fmt.Errorf("containerd version: %w", err)
	}
	if err := checkParallelUnpack(version.Version); err != nil {
		return err
	}

	src, err := registry.NewOCIRegistry(ctx, ref)
	if err != nil {
		return fmt.Errorf("registry %s: %w", ref, err)
	}
	dst := transferimage.NewStore(ref,
		transferimage.WithPlatforms(linuxAMD64),
		transferimage.WithUnpack(linuxAMD64, dockerSnapshotter),
	)
	start := p.now()
	agg := newTransferAggregator(p.now)
	err = client.Transfer(ctx, src, dst, transfer.WithProgress(func(tp transfer.Progress) {
		if agg.apply(tp) && onProgress != nil {
			onProgress(agg.progress())
		}
	}))
	if err != nil {
		return err
	}
	p.log.Info("image pulled through containerd", "image", ref, "layers", len(agg.layers), "mb", agg.totalBytes()/1_000_000,
		"download_s", seconds(agg.downloadedIn(start)), "total_s", seconds(p.now().Sub(start)))
	return nil
}

type transferAggregator struct {
	layers       map[string]*layerState
	order        []string
	lastPct      int
	lastDone     int
	now          func() time.Time
	downloadedAt time.Time
}

func newTransferAggregator(now func() time.Time) *transferAggregator {
	return &transferAggregator{layers: map[string]*layerState{}, now: now}
}

func (a *transferAggregator) downloadedIn(start time.Time) time.Duration {
	if a.downloadedAt.IsZero() {
		return 0
	}
	return a.downloadedAt.Sub(start)
}

func (a *transferAggregator) totalBytes() int64 {
	var sum int64
	for _, ls := range a.layers {
		sum += ls.total
	}
	return sum
}

func (a *transferAggregator) markDownloaded(ls *layerState) {
	if ls.downloaded {
		return
	}
	ls.downloaded = true
	for _, other := range a.layers {
		if !other.downloaded {
			return
		}
	}
	a.downloadedAt = a.now()
}

func isLayer(tp transfer.Progress) bool {
	return tp.Desc != nil && strings.Contains(tp.Desc.MediaType, "layer")
}

func (a *transferAggregator) apply(tp transfer.Progress) bool {
	if !isLayer(tp) {
		return false
	}
	ls := a.layers[tp.Name]
	if ls == nil {
		ls = &layerState{}
		a.layers[tp.Name] = ls
		a.order = append(a.order, tp.Name)
		a.downloadedAt = time.Time{}
	}
	if tp.Total > 0 {
		ls.total = tp.Total
	} else if ls.total == 0 {
		ls.total = tp.Desc.Size
	}
	if tp.Progress > ls.current {
		ls.current = tp.Progress
	}
	switch tp.Event {
	case "complete", "extracting":
		ls.current = ls.total
		a.markDownloaded(ls)
	case "extracted", "already exists":
		ls.current = ls.total
		ls.done = true
		a.markDownloaded(ls)
	}
	pct, done := a.compute()
	if pct != a.lastPct || done != a.lastDone {
		a.lastPct, a.lastDone = pct, done
		return true
	}
	return false
}

func (a *transferAggregator) compute() (pct, done int) {
	var sumCur, sumTot int64
	for _, ls := range a.layers {
		sumCur += ls.current
		sumTot += ls.total
		if ls.done {
			done++
		}
	}
	if sumTot > 0 {
		pct = int(sumCur * 100 / sumTot)
	}
	if pct > 100 {
		pct = 100
	}
	return pct, done
}

func (a *transferAggregator) progress() PullProgress {
	return PullProgress{Percent: a.lastPct, LayersDone: a.lastDone, LayersTotal: len(a.layers)}
}

type ImageStore interface {
	UsesContainerdStore(ctx context.Context) (bool, error)
}

type FallbackPuller struct {
	fast   Puller
	docker Puller
	store  ImageStore
	log    *slog.Logger
}

func NewFallbackPuller(fast, docker Puller, store ImageStore, log *slog.Logger) *FallbackPuller {
	return &FallbackPuller{fast: fast, docker: docker, store: store, log: log}
}

func (p *FallbackPuller) Pull(ctx context.Context, image string, onProgress func(PullProgress)) error {
	ok, err := p.store.UsesContainerdStore(ctx)
	switch {
	case err != nil:
		p.log.Warn("docker image store unknown, pulling through docker", "err", err)
	case !ok:
		p.log.Info("docker keeps images in its own store, pulling through docker")
	default:
		if err := p.fast.Pull(ctx, image, onProgress); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return err
		} else {
			p.log.Warn("containerd pull failed, pulling through docker", "image", image, "err", err)
		}
	}
	start := time.Now()
	if err := p.docker.Pull(ctx, image, onProgress); err != nil {
		return err
	}
	p.log.Info("image pulled through docker", "image", image, "total_s", seconds(time.Since(start)))
	return nil
}

func (p *SocketPuller) UsesContainerdStore(ctx context.Context) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/info", nil)
	if err != nil {
		return false, err
	}
	resp, err := p.httpc.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("docker info http %d", resp.StatusCode)
	}
	var info struct {
		DriverStatus [][2]string `json:"DriverStatus"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return false, err
	}
	for _, kv := range info.DriverStatus {
		if kv[0] == "driver-type" && kv[1] == containerdStoreType {
			return true, nil
		}
	}
	return false, nil
}
