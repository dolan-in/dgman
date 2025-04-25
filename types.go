package dgman

import (
	"math/big"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
}.Froze()

// timeEncoder is a custom JSON encoder for time.Time values
type timeEncoder struct{}

func (e *timeEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	return (*time.Time)(ptr).IsZero()
}

// Encode encodes a time.Time value as a JSON string if not a "Zero" time
func (e *timeEncoder) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	t := *(*time.Time)(ptr)
	if t.IsZero() {
		stream.WriteNil()
	} else {
		stream.WriteString(t.Format(time.RFC3339))
	}
}

type bigFloatEncoder struct{}

func (e *bigFloatEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	f := (*big.Float)(ptr)
	return f == nil || f.Sign() == 0
}

func (e *bigFloatEncoder) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	f := (*big.Float)(ptr)
	if f == nil || f.Sign() == 0 {
		stream.WriteNil()
	} else {
		stream.WriteString(f.Text('f', -1))
	}
}

type bigFloatDecoder struct{}

func (d *bigFloatDecoder) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	switch iter.WhatIsNext() {
	case jsoniter.NilValue:
		iter.ReadNil()
		*(*big.Float)(ptr) = *big.NewFloat(0)
	case jsoniter.StringValue:
		str := iter.ReadString()
		f, _, err := big.ParseFloat(str, 10, 0, big.ToNearestEven)
		if err != nil {
			iter.ReportError("decode big.Float", err.Error())
			return
		}
		*(*big.Float)(ptr) = *f
	case jsoniter.NumberValue:
		str := iter.ReadNumber().String()
		f, _, err := big.ParseFloat(str, 10, 0, big.ToNearestEven)
		if err != nil {
			iter.ReportError("decode big.Float", err.Error())
			return
		}
		*(*big.Float)(ptr) = *f
	default:
		iter.ReportError("decode big.Float", "invalid value type")
	}
}

// VectorFloat32 represents a float32vector in Dgraph
type VectorFloat32 struct {
	Values []float32
}

// SchemaType implements SchemaType interface to provide the Dgraph type
func (v VectorFloat32) SchemaType() string {
	return "float32vector"
}

// vectorFloat32Encoder encodes VectorFloat32 as a quoted JSON array string
type vectorFloat32Encoder struct{}

func (e *vectorFloat32Encoder) IsEmpty(ptr unsafe.Pointer) bool {
	v := (*VectorFloat32)(ptr)
	return v == nil || len(v.Values) == 0
}

func (e *vectorFloat32Encoder) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	v := (*VectorFloat32)(ptr)
	if v == nil || len(v.Values) == 0 {
		stream.WriteNil()
		return
	}

	// Convert the Values slice to a JSON array as a string
	arrayBytes, err := json.Marshal(v.Values)
	if err != nil {
		stream.Error = err
		return
	}

	// Write the JSON array as a quoted string
	stream.WriteString(string(arrayBytes))
}

// vectorFloat32Decoder decodes a quoted JSON array string into VectorFloat32
type vectorFloat32Decoder struct{}

func (d *vectorFloat32Decoder) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	v := (*VectorFloat32)(ptr)

	switch iter.WhatIsNext() {
	case jsoniter.NilValue:
		iter.ReadNil()
		v.Values = []float32{}
	case jsoniter.StringValue:
		// Handle string-encoded arrays: "[]" or "[1.0,2.0,3.0]"
		str := iter.ReadString()

		// Strip quotes if they're the outermost characters
		if len(str) >= 2 && str[0] == '"' && str[len(str)-1] == '"' {
			str = str[1 : len(str)-1]
		}

		if err := json.Unmarshal([]byte(str), &v.Values); err != nil {
			iter.ReportError("decode VectorFloat32", err.Error())
		}
	case jsoniter.ArrayValue:
		// Handle direct array: [] or [1.0,2.0,3.0]
		var values []float32
		iter.ReadVal(&values)
		v.Values = values
	default:
		iter.ReportError("decode VectorFloat32", "invalid value type")
	}
}

func init() {
	jsoniter.RegisterTypeEncoder("time.Time", &timeEncoder{})
	jsoniter.RegisterTypeEncoder("big.Float", &bigFloatEncoder{})
	jsoniter.RegisterTypeDecoder("big.Float", &bigFloatDecoder{})
	jsoniter.RegisterTypeEncoder("dgman.VectorFloat32", &vectorFloat32Encoder{})
	jsoniter.RegisterTypeDecoder("dgman.VectorFloat32", &vectorFloat32Decoder{})
}
