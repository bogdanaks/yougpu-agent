package reconcile

import "github.com/bogdanaks/yougpu-agent/internal/client"

// Reconcile returns the list of actions needed to bring observed state into spec.
// Pure function: no I/O, fully unit-testable.
func Reconcile(spec *client.AgentSpec, observed ObservedState) []Action {
	if spec == nil {
		return nil
	}

	specIDs := make(map[string]bool, len(spec.Disks))
	var actions []Action

	for _, disk := range spec.Disks {
		specIDs[disk.ID] = true

		switch disk.DesiredState {
		case client.DesiredMounted:
			if !observed.MountedDiskIDs[disk.ID] {
				actions = append(actions, MountDisk{Spec: disk})
			}
		case client.DesiredUnmounted:
			if observed.MountedDiskIDs[disk.ID] || observed.UnitDiskIDs[disk.ID] {
				actions = append(actions, UnmountDisk{ID: disk.ID})
			}
		}
	}

	for id := range observed.UnitDiskIDs {
		if !specIDs[id] {
			actions = append(actions, UnmountOrphan{ID: id})
		}
	}

	return actions
}
