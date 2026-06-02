package reconcile

import "github.com/bogdanaks/yougpu-agent/internal/client"

type Action interface {
	Kind() string
}

type MountDisk struct {
	Spec client.AgentDiskSpec
}

func (MountDisk) Kind() string { return "mount" }

type UnmountDisk struct {
	ID string
}

func (UnmountDisk) Kind() string { return "unmount" }

type UnmountOrphan struct {
	ID string
}

func (UnmountOrphan) Kind() string { return "unmount_orphan" }

type ObservedState struct {
	MountedDiskIDs map[string]bool
	UnitDiskIDs    map[string]bool
}
