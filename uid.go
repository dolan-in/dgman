package dgman

import (
	"regexp"
	"strings"
)

var (
	uidCleanerRegex = regexp.MustCompile("[^x0-9]+")

	_ ParamFormatter = (*UID)(nil)
	_ ParamFormatter = (*UIDs)(nil)
)

// UID type allows passing uid's as query parameters
type UID string

// FormatParams implements the ParamFormatter interface
func (u UID) FormatParams() []byte {
	return uidCleanerRegex.ReplaceAll([]byte(u), nil)
}

// UIDs type allows passing list of uid's as query parameters
type UIDs []string

// FormatParams implements the ParamFormatter interface
func (u UIDs) FormatParams() []byte {
	uids := []string(u)
	for idx, uid := range uids {
		uids[idx] = uidCleanerRegex.ReplaceAllString(uid, "")
	}
	return []byte(strings.Join(uids, ", "))
}
