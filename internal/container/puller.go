package container

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const dockerSocket = "/var/run/docker.sock"

type PullProgress struct {
	Percent     int
	LayersDone  int
	LayersTotal int
}

type Puller interface {
	Pull(ctx context.Context, image string, onProgress func(PullProgress)) error
}

type SocketPuller struct {
	httpc *http.Client
}

func NewSocketPuller() *SocketPuller {
	return &SocketPuller{
		httpc: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", dockerSocket)
				},
			},
		},
	}
}

func (p *SocketPuller) Pull(ctx context.Context, image string, onProgress func(PullProgress)) error {
	repo, tag := splitImageRef(image)
	q := url.Values{}
	q.Set("fromImage", repo)
	if tag != "" {
		q.Set("tag", tag)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker/images/create?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := p.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("images/create http %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	agg := newPullAggregator()
	dec := json.NewDecoder(resp.Body)
	for {
		var msg pullMessage
		if err := dec.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decode pull stream: %w", err)
		}
		if msg.Error != "" {
			return fmt.Errorf("pull failed: %s", msg.Error)
		}
		if agg.apply(msg) && onProgress != nil {
			onProgress(agg.progress())
		}
	}
	return nil
}

func splitImageRef(image string) (repo, tag string) {
	if at := strings.LastIndex(image, "@"); at != -1 {
		return image[:at], image[at+1:]
	}
	colon := strings.LastIndex(image, ":")
	slash := strings.LastIndex(image, "/")
	if colon > slash {
		return image[:colon], image[colon+1:]
	}
	return image, "latest"
}

type pullMessage struct {
	Status         string `json:"status"`
	ID             string `json:"id"`
	Error          string `json:"error"`
	ProgressDetail struct {
		Current int64 `json:"current"`
		Total   int64 `json:"total"`
	} `json:"progressDetail"`
}

type layerState struct {
	current int64
	total   int64
	done    bool
}

type pullAggregator struct {
	layers   map[string]*layerState
	lastPct  int
	lastDone int
}

func newPullAggregator() *pullAggregator {
	return &pullAggregator{layers: map[string]*layerState{}}
}

func (a *pullAggregator) apply(msg pullMessage) bool {
	if msg.ID == "" {
		return false
	}
	ls := a.layers[msg.ID]
	if ls == nil {
		ls = &layerState{}
		a.layers[msg.ID] = ls
	}
	switch msg.Status {
	case "Downloading":
		if msg.ProgressDetail.Total > 0 {
			ls.total = msg.ProgressDetail.Total
		}
		if msg.ProgressDetail.Current > ls.current {
			ls.current = msg.ProgressDetail.Current
		}
	case "Verifying Checksum", "Download complete", "Extracting":
		if ls.total > 0 {
			ls.current = ls.total
		}
	case "Pull complete":
		if ls.total > 0 {
			ls.current = ls.total
		}
		ls.done = true
	case "Already exists":
		ls.done = true
	}
	pct, done := a.compute()
	if pct != a.lastPct || done != a.lastDone {
		a.lastPct, a.lastDone = pct, done
		return true
	}
	return false
}

func (a *pullAggregator) compute() (pct, done int) {
	var sumCur, sumTot int64
	for _, ls := range a.layers {
		sumCur += ls.current
		sumTot += ls.total
		if ls.done {
			done++
		}
	}
	switch {
	case sumTot > 0:
		pct = int(sumCur * 100 / sumTot)
	case len(a.layers) > 0:
		pct = done * 100 / len(a.layers)
	}
	if pct > 100 {
		pct = 100
	}
	return pct, done
}

func (a *pullAggregator) progress() PullProgress {
	return PullProgress{Percent: a.lastPct, LayersDone: a.lastDone, LayersTotal: len(a.layers)}
}
