// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of KhulnaSoft
package workers

// export for testing only

// TasksCap returns the tasks channel capacity.
func (wp *Workers) TasksCap() int {
	return cap(wp.tasks)
}
