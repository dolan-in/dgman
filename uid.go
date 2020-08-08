/*
 * Copyright (C) 2019 Dolan and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dgman

import (
	"regexp"
	"strings"
)

var (
	uidCleanerRegex = regexp.MustCompile("[^xa-fA-F0-9]+")

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
