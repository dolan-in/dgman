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
