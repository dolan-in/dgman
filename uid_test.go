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
	"reflect"
	"testing"
)

func TestUID_FormatParams(t *testing.T) {
	tests := []struct {
		name string
		u    UID
		want []byte
	}{
		{
			name: "should remove all unknown characters",
			u:    UID("0px123jg"),
			want: []byte("0x123"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.u.FormatParams(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UID.FormatParams() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestUIDs_FormatParams(t *testing.T) {
	tests := []struct {
		name string
		u    UIDs
		want []byte
	}{
		{
			name: "should parse list of uids",
			u:    UIDs{"0px123jk", "0px123jk"},
			want: []byte("0x123, 0x123"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.u.FormatParams(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UIDs.FormatParams() = %s, want %s", got, tt.want)
			}
		})
	}
}
