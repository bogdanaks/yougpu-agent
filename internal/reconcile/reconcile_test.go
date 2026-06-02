package reconcile

import (
	"sort"
	"testing"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

func TestReconcile(t *testing.T) {
	mk := func(id, desired string) client.AgentDiskSpec {
		return client.AgentDiskSpec{
			ID:           id,
			DesiredState: desired,
			Bucket:       "yougpu-r2",
			S3Path:       "u/" + id + "/",
			MountPath:    "/workspace/storage/" + id,
		}
	}

	tests := []struct {
		name     string
		spec     *client.AgentSpec
		observed ObservedState
		want     []string // canonical "kind:id" strings
	}{
		{
			name:     "nil spec → no actions",
			spec:     nil,
			observed: ObservedState{},
			want:     nil,
		},
		{
			name:     "empty spec, nothing mounted → no actions",
			spec:     &client.AgentSpec{Generation: 1},
			observed: ObservedState{},
			want:     nil,
		},
		{
			name:     "new disk in spec, nothing mounted → MountDisk",
			spec:     &client.AgentSpec{Disks: []client.AgentDiskSpec{mk("a", "mounted")}},
			observed: ObservedState{},
			want:     []string{"mount:a"},
		},
		{
			name:     "disk already mounted, still desired → no-op",
			spec:     &client.AgentSpec{Disks: []client.AgentDiskSpec{mk("a", "mounted")}},
			observed: ObservedState{MountedDiskIDs: map[string]bool{"a": true}, UnitDiskIDs: map[string]bool{"a": true}},
			want:     nil,
		},
		{
			name: "disk removed from spec, unit still present → UnmountOrphan",
			spec: &client.AgentSpec{Disks: []client.AgentDiskSpec{}},
			observed: ObservedState{
				MountedDiskIDs: map[string]bool{"a": true},
				UnitDiskIDs:    map[string]bool{"a": true},
			},
			want: []string{"unmount_orphan:a"},
		},
		{
			name: "disk desired=unmounted, currently mounted → UnmountDisk",
			spec: &client.AgentSpec{Disks: []client.AgentDiskSpec{mk("a", "unmounted")}},
			observed: ObservedState{
				MountedDiskIDs: map[string]bool{"a": true},
				UnitDiskIDs:    map[string]bool{"a": true},
			},
			want: []string{"unmount:a"},
		},
		{
			name:     "disk desired=unmounted, never mounted → no-op",
			spec:     &client.AgentSpec{Disks: []client.AgentDiskSpec{mk("a", "unmounted")}},
			observed: ObservedState{},
			want:     nil,
		},
		{
			name: "mix: one new, one stable, one orphan",
			spec: &client.AgentSpec{Disks: []client.AgentDiskSpec{
				mk("a", "mounted"), // already mounted
				mk("b", "mounted"), // new, must mount
			}},
			observed: ObservedState{
				MountedDiskIDs: map[string]bool{"a": true, "orphan": true},
				UnitDiskIDs:    map[string]bool{"a": true, "orphan": true},
			},
			want: []string{"mount:b", "unmount_orphan:orphan"},
		},
		{
			name: "unit exists but not active, desired=mounted → MountDisk (restart)",
			spec: &client.AgentSpec{Disks: []client.AgentDiskSpec{mk("a", "mounted")}},
			observed: ObservedState{
				MountedDiskIDs: map[string]bool{},
				UnitDiskIDs:    map[string]bool{"a": true},
			},
			want: []string{"mount:a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canonical(Reconcile(tt.spec, tt.observed))
			want := append([]string{}, tt.want...)
			sort.Strings(want)
			if !equal(got, want) {
				t.Fatalf("\nwant: %v\ngot:  %v", want, got)
			}
		})
	}
}

func canonical(actions []Action) []string {
	out := make([]string, 0, len(actions))
	for _, a := range actions {
		switch v := a.(type) {
		case MountDisk:
			out = append(out, "mount:"+v.Spec.ID)
		case UnmountDisk:
			out = append(out, "unmount:"+v.ID)
		case UnmountOrphan:
			out = append(out, "unmount_orphan:"+v.ID)
		}
	}
	sort.Strings(out)
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
