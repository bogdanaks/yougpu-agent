package container

import "testing"

func TestSplitImageRef(t *testing.T) {
	cases := []struct {
		in       string
		wantRepo string
		wantTag  string
	}{
		{"ubuntu", "ubuntu", "latest"},
		{"pytorch/pytorch:2.0", "pytorch/pytorch", "2.0"},
		{"registry:5000/repo", "registry:5000/repo", "latest"},
		{"registry:5000/repo:v1", "registry:5000/repo", "v1"},
		{"repo@sha256:abc", "repo", "sha256:abc"},
	}
	for _, c := range cases {
		repo, tag := splitImageRef(c.in)
		if repo != c.wantRepo || tag != c.wantTag {
			t.Errorf("splitImageRef(%q) = %q,%q want %q,%q", c.in, repo, tag, c.wantRepo, c.wantTag)
		}
	}
}

func TestAggregatorIgnoresHeaderLines(t *testing.T) {
	a := newPullAggregator()
	if a.apply(pullMessage{Status: "Pulling from library/redis"}) {
		t.Fatal("header line (no id) must not produce progress")
	}
}

func TestAggregatorBytePercent(t *testing.T) {
	a := newPullAggregator()
	a.apply(pullMessage{ID: "a", Status: "Pulling fs layer"})
	a.apply(pullMessage{ID: "b", Status: "Pulling fs layer"})

	msg := pullMessage{ID: "a", Status: "Downloading"}
	msg.ProgressDetail.Current = 50
	msg.ProgressDetail.Total = 100
	a.apply(msg)
	msg = pullMessage{ID: "b", Status: "Downloading"}
	msg.ProgressDetail.Current = 50
	msg.ProgressDetail.Total = 100
	a.apply(msg)

	p := a.progress()
	if p.Percent != 50 || p.LayersTotal != 2 || p.LayersDone != 0 {
		t.Fatalf("got %+v, want 50%% 0/2", p)
	}

	a.apply(pullMessage{ID: "a", Status: "Pull complete"})
	a.apply(pullMessage{ID: "b", Status: "Pull complete"})
	p = a.progress()
	if p.Percent != 100 || p.LayersDone != 2 {
		t.Fatalf("got %+v, want 100%% 2/2", p)
	}
}

func TestAggregatorAlreadyExistsCountsAsDone(t *testing.T) {
	a := newPullAggregator()
	a.apply(pullMessage{ID: "a", Status: "Already exists"})
	a.apply(pullMessage{ID: "b", Status: "Already exists"})
	p := a.progress()
	if p.Percent != 100 || p.LayersDone != 2 || p.LayersTotal != 2 {
		t.Fatalf("got %+v, want 100%% 2/2 (layer-based fallback)", p)
	}
}

func TestAggregatorChangeDetection(t *testing.T) {
	a := newPullAggregator()
	msg := pullMessage{ID: "a", Status: "Downloading"}
	msg.ProgressDetail.Current = 10
	msg.ProgressDetail.Total = 100
	if !a.apply(msg) {
		t.Fatal("first progress must report change")
	}
	if a.apply(msg) {
		t.Fatal("identical progress must not report change")
	}
}
