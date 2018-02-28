// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: roachpb/internal.proto

package roachpb

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

import binary "encoding/binary"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// InternalTimeSeriesData is a collection of data samples for some
// measurable value, where each sample is taken over a uniform time
// interval.
//
// The collection itself contains a start timestamp (in seconds since the unix
// epoch) and a sample duration (in milliseconds). Each sample in the collection
// will contain a positive integer offset that indicates the length of time
// between the start_timestamp of the collection and the time when the sample
// began, expressed as an whole number of sample intervals. For example, if the
// sample duration is 60000 (indicating 1 minute), then a contained sample with
// an offset value of 5 begins (5*60000ms = 300000ms = 5 minutes) after the
// start timestamp of this data.
//
// This is meant to be an efficient internal representation of time series data,
// ensuring that very little redundant data is stored on disk. With this goal in
// mind, this message does not identify the variable which is actually being
// measured; that information is expected be encoded in the key where this
// message is stored.
type InternalTimeSeriesData struct {
	// Holds a wall time, expressed as a unix epoch time in nanoseconds. This
	// represents the earliest possible timestamp for a sample within the
	// collection.
	StartTimestampNanos int64 `protobuf:"varint,1,opt,name=start_timestamp_nanos,json=startTimestampNanos" json:"start_timestamp_nanos"`
	// The duration of each sample interval, expressed in nanoseconds.
	SampleDurationNanos int64 `protobuf:"varint,2,opt,name=sample_duration_nanos,json=sampleDurationNanos" json:"sample_duration_nanos"`
	// The actual data samples for this metric.
	Samples              []InternalTimeSeriesSample `protobuf:"bytes,3,rep,name=samples" json:"samples"`
	XXX_NoUnkeyedLiteral struct{}                   `json:"-"`
	XXX_sizecache        int32                      `json:"-"`
}

func (m *InternalTimeSeriesData) Reset()                    { *m = InternalTimeSeriesData{} }
func (m *InternalTimeSeriesData) String() string            { return proto.CompactTextString(m) }
func (*InternalTimeSeriesData) ProtoMessage()               {}
func (*InternalTimeSeriesData) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{0} }
func (dst *InternalTimeSeriesData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_InternalTimeSeriesData.Merge(dst, src)
}
func (m *InternalTimeSeriesData) XXX_Size() int {
	return xxx_messageInfo_InternalTimeSeriesData.Size(m)
}
func (m *InternalTimeSeriesData) XXX_DiscardUnknown() {
	xxx_messageInfo_InternalTimeSeriesData.DiscardUnknown(m)
}

var xxx_messageInfo_InternalTimeSeriesData proto.InternalMessageInfo

// A InternalTimeSeriesSample represents data gathered from multiple
// measurements of a variable value over a given period of time. The
// length of that period of time is stored in an
// InternalTimeSeriesData message; a sample cannot be interpreted
// correctly without a start timestamp and sample duration.
//
// Each sample may contain data gathered from multiple measurements of the same
// variable, as long as all of those measurements occurred within the sample
// period. The sample stores several aggregated values from these measurements:
// - The sum of all measured values
// - A count of all measurements taken
// - The maximum individual measurement seen
// - The minimum individual measurement seen
//
// If zero measurements are present in a sample, then it should be omitted
// entirely from any collection it would be a part of.
//
// If the count of measurements is 1, then max and min fields may be omitted
// and assumed equal to the sum field.
type InternalTimeSeriesSample struct {
	// Temporal offset from the "start_timestamp" of the InternalTimeSeriesData
	// collection this data point is part in. The units of this value are
	// determined by the value of the "sample_duration_milliseconds" field of
	// the TimeSeriesData collection.
	Offset int32 `protobuf:"varint,1,opt,name=offset" json:"offset"`
	// Sum of all measurements.
	Sum float64 `protobuf:"fixed64,7,opt,name=sum" json:"sum"`
	// Count of measurements taken within this sample.
	Count uint32 `protobuf:"varint,6,opt,name=count" json:"count"`
	// Maximum encountered measurement in this sample.
	Max *float64 `protobuf:"fixed64,8,opt,name=max" json:"max,omitempty"`
	// Minimum encountered measurement in this sample.
	Min                  *float64 `protobuf:"fixed64,9,opt,name=min" json:"min,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *InternalTimeSeriesSample) Reset()                    { *m = InternalTimeSeriesSample{} }
func (m *InternalTimeSeriesSample) String() string            { return proto.CompactTextString(m) }
func (*InternalTimeSeriesSample) ProtoMessage()               {}
func (*InternalTimeSeriesSample) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{1} }
func (dst *InternalTimeSeriesSample) XXX_Merge(src proto.Message) {
	xxx_messageInfo_InternalTimeSeriesSample.Merge(dst, src)
}
func (m *InternalTimeSeriesSample) XXX_Size() int {
	return xxx_messageInfo_InternalTimeSeriesSample.Size(m)
}
func (m *InternalTimeSeriesSample) XXX_DiscardUnknown() {
	xxx_messageInfo_InternalTimeSeriesSample.DiscardUnknown(m)
}

var xxx_messageInfo_InternalTimeSeriesSample proto.InternalMessageInfo

func init() {
	proto.RegisterType((*InternalTimeSeriesData)(nil), "cockroach.roachpb.InternalTimeSeriesData")
	proto.RegisterType((*InternalTimeSeriesSample)(nil), "cockroach.roachpb.InternalTimeSeriesSample")
}
func (m *InternalTimeSeriesData) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *InternalTimeSeriesData) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintInternal(dAtA, i, uint64(m.StartTimestampNanos))
	dAtA[i] = 0x10
	i++
	i = encodeVarintInternal(dAtA, i, uint64(m.SampleDurationNanos))
	if len(m.Samples) > 0 {
		for _, msg := range m.Samples {
			dAtA[i] = 0x1a
			i++
			i = encodeVarintInternal(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *InternalTimeSeriesSample) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *InternalTimeSeriesSample) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintInternal(dAtA, i, uint64(m.Offset))
	dAtA[i] = 0x30
	i++
	i = encodeVarintInternal(dAtA, i, uint64(m.Count))
	dAtA[i] = 0x39
	i++
	binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(m.Sum))))
	i += 8
	if m.Max != nil {
		dAtA[i] = 0x41
		i++
		binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(*m.Max))))
		i += 8
	}
	if m.Min != nil {
		dAtA[i] = 0x49
		i++
		binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(*m.Min))))
		i += 8
	}
	return i, nil
}

func encodeVarintInternal(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *InternalTimeSeriesData) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovInternal(uint64(m.StartTimestampNanos))
	n += 1 + sovInternal(uint64(m.SampleDurationNanos))
	if len(m.Samples) > 0 {
		for _, e := range m.Samples {
			l = e.Size()
			n += 1 + l + sovInternal(uint64(l))
		}
	}
	return n
}

func (m *InternalTimeSeriesSample) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovInternal(uint64(m.Offset))
	n += 1 + sovInternal(uint64(m.Count))
	n += 9
	if m.Max != nil {
		n += 9
	}
	if m.Min != nil {
		n += 9
	}
	return n
}

func sovInternal(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozInternal(x uint64) (n int) {
	return sovInternal(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *InternalTimeSeriesData) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowInternal
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: InternalTimeSeriesData: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: InternalTimeSeriesData: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field StartTimestampNanos", wireType)
			}
			m.StartTimestampNanos = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.StartTimestampNanos |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SampleDurationNanos", wireType)
			}
			m.SampleDurationNanos = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.SampleDurationNanos |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Samples", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthInternal
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Samples = append(m.Samples, InternalTimeSeriesSample{})
			if err := m.Samples[len(m.Samples)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipInternal(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthInternal
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *InternalTimeSeriesSample) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowInternal
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: InternalTimeSeriesSample: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: InternalTimeSeriesSample: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Offset", wireType)
			}
			m.Offset = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Offset |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Count", wireType)
			}
			m.Count = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Count |= (uint32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 7:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field Sum", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			m.Sum = float64(math.Float64frombits(v))
		case 8:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field Max", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			m.Max = &v2
		case 9:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field Min", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			m.Min = &v2
		default:
			iNdEx = preIndex
			skippy, err := skipInternal(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthInternal
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipInternal(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowInternal
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthInternal
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipInternal(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthInternal = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowInternal   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("roachpb/internal.proto", fileDescriptorInternal) }

var fileDescriptorInternal = []byte{
	// 300 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x8f, 0xc1, 0x4a, 0xc3, 0x30,
	0x18, 0x80, 0x1b, 0xeb, 0x56, 0x8d, 0x08, 0x1a, 0xb5, 0x84, 0x21, 0xb1, 0xee, 0x54, 0x10, 0x3a,
	0xf0, 0xe4, 0x79, 0xec, 0x22, 0x82, 0x87, 0x6d, 0x27, 0x2f, 0x25, 0xd6, 0x6c, 0x06, 0x97, 0xa4,
	0x24, 0x29, 0xf8, 0x18, 0x7b, 0xac, 0x1e, 0x3d, 0xea, 0x45, 0xb4, 0xbe, 0x88, 0x34, 0xcd, 0x0e,
	0x32, 0xbc, 0x85, 0xef, 0xff, 0xbe, 0x9f, 0xfc, 0x30, 0xd6, 0x8a, 0x16, 0xcf, 0xe5, 0xe3, 0x88,
	0x4b, 0xcb, 0xb4, 0xa4, 0xab, 0xac, 0xd4, 0xca, 0x2a, 0x74, 0x5c, 0xa8, 0xe2, 0xc5, 0xcd, 0x32,
	0x6f, 0x0c, 0x4e, 0x97, 0x6a, 0xa9, 0xdc, 0x74, 0xd4, 0xbe, 0x3a, 0x71, 0xf8, 0x01, 0x60, 0x7c,
	0xeb, 0xdb, 0x39, 0x17, 0x6c, 0xc6, 0x34, 0x67, 0x66, 0x42, 0x2d, 0x45, 0x37, 0xf0, 0xcc, 0x58,
	0xaa, 0x6d, 0x6e, 0xb9, 0x60, 0xc6, 0x52, 0x51, 0xe6, 0x92, 0x4a, 0x65, 0x30, 0x48, 0x40, 0x1a,
	0x8e, 0x77, 0xeb, 0xcf, 0x8b, 0x60, 0x7a, 0xe2, 0x94, 0xf9, 0xc6, 0xb8, 0x6f, 0x05, 0x57, 0x52,
	0x51, 0xae, 0x58, 0xfe, 0x54, 0x69, 0x6a, 0xb9, 0x92, 0xbe, 0xdc, 0xf9, 0x53, 0x3a, 0x65, 0xe2,
	0x8d, 0xae, 0xbc, 0x83, 0x51, 0x87, 0x0d, 0x0e, 0x93, 0x30, 0x3d, 0xb8, 0xbe, 0xca, 0xb6, 0x2e,
	0xc9, 0xb6, 0xff, 0x3b, 0x73, 0x8d, 0x5f, 0xbc, 0xd9, 0x30, 0x5c, 0x03, 0x88, 0xff, 0x73, 0xd1,
	0x39, 0xec, 0xab, 0xc5, 0xc2, 0x30, 0xeb, 0xce, 0xe9, 0xf9, 0xd6, 0x33, 0x34, 0x80, 0xbd, 0x42,
	0x55, 0xd2, 0xe2, 0x7e, 0x02, 0xd2, 0x43, 0x3f, 0xec, 0x10, 0x8a, 0x61, 0x68, 0x2a, 0x81, 0xa3,
	0x04, 0xa4, 0xc0, 0x4f, 0x5a, 0x80, 0x8e, 0x60, 0x28, 0xe8, 0x2b, 0xde, 0x6b, 0xf9, 0xb4, 0x7d,
	0x3a, 0xc2, 0x25, 0xde, 0xf7, 0x84, 0xcb, 0xf1, 0x65, 0xfd, 0x4d, 0x82, 0xba, 0x21, 0xe0, 0xad,
	0x21, 0xe0, 0xbd, 0x21, 0xe0, 0xab, 0x21, 0x60, 0xfd, 0x43, 0x82, 0x87, 0xc8, 0x5f, 0xf7, 0x1b,
	0x00, 0x00, 0xff, 0xff, 0xff, 0xd1, 0x2a, 0x47, 0xd3, 0x01, 0x00, 0x00,
}
