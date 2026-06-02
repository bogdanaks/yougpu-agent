// Package reconcile вычисляет список Action'ов, необходимых чтобы привести наблюдаемое
// состояние VM к желаемому, описанному в AgentSpec. Чистая функция без I/O — всё
// тестируется в reconcile_test.go без mocks systemd/exec.
package reconcile

import "github.com/bogdanaks/yougpu-agent/internal/client"

// Reconcile сравнивает spec и observed, возвращает план действий.
//
// Семантика lifecycle: если backend выставил deletion_requested_at, mount/unmount-действия
// НЕ генерируются — на этом этапе агент готовится к sync'у, новые маунты бессмысленны
// (всё равно через минуту будем стопать). Этот фильтр обеспечивается caller'ом (agent.Run),
// который не зовёт Reconcile когда lifecycle ≠ alive.
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

	// Orphan unit cleanup: unit'ы для дисков, исчезнувших из spec (detached в БД).
	for id := range observed.UnitDiskIDs {
		if !specIDs[id] {
			actions = append(actions, UnmountOrphan{ID: id})
		}
	}

	return actions
}
