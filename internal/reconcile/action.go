package reconcile

import "github.com/bogdanaks/yougpu-agent/internal/client"

// Action — sum-type через interface. Каждый вариант реализует Kind().
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

// UnmountOrphan — unit'ы для дисков которых уже нет в spec (detached/удалены backend'ом).
type UnmountOrphan struct {
	ID string
}

func (UnmountOrphan) Kind() string { return "unmount_orphan" }

// ObservedState описывает то, что агент знает о текущем состоянии VM.
// IDs — drive_id, не имена unit'ов.
type ObservedState struct {
	// MountedDiskIDs — диски, для которых systemctl is-active вернул "active".
	MountedDiskIDs map[string]bool
	// UnitDiskIDs — все drive_id, для которых существует /etc/systemd/system/yougpu-storage-*.service.
	// Используется чтобы заметить unit'ы, оставшиеся от исчезнувших из spec дисков.
	UnitDiskIDs map[string]bool
}
