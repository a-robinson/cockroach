// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: storage/liveness.proto

package storage

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"
import cockroach_util_hlc1 "github.com/cockroachdb/cockroach/pkg/util/hlc"

import github_com_cockroachdb_cockroach_pkg_roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// NodeLivenessStatus describes the status of a node from the perspective of the
// liveness system.
type NodeLivenessStatus int32

const (
	NodeLivenessStatus_UNKNOWN NodeLivenessStatus = 0
	// DEAD indicates the node is considered dead.
	NodeLivenessStatus_DEAD NodeLivenessStatus = 1
	// UNAVAILABLE indicates that the node is unavailable - it has not updated its
	// liveness record recently enough to be considered live, but has not been
	// unavailable long enough to be considered dead.
	NodeLivenessStatus_UNAVAILABLE NodeLivenessStatus = 2
	// LIVE indicates a live node.
	NodeLivenessStatus_LIVE NodeLivenessStatus = 3
	// DECOMMISSIONING indicates a node that is in the decommissioning process.
	NodeLivenessStatus_DECOMMISSIONING NodeLivenessStatus = 4
	// DECOMMISSIONED indicates a node that has finished the decommissioning
	// process.
	NodeLivenessStatus_DECOMMISSIONED NodeLivenessStatus = 5
)

var NodeLivenessStatus_name = map[int32]string{
	0: "UNKNOWN",
	1: "DEAD",
	2: "UNAVAILABLE",
	3: "LIVE",
	4: "DECOMMISSIONING",
	5: "DECOMMISSIONED",
}
var NodeLivenessStatus_value = map[string]int32{
	"UNKNOWN":         0,
	"DEAD":            1,
	"UNAVAILABLE":     2,
	"LIVE":            3,
	"DECOMMISSIONING": 4,
	"DECOMMISSIONED":  5,
}

func (x NodeLivenessStatus) String() string {
	return proto.EnumName(NodeLivenessStatus_name, int32(x))
}
func (NodeLivenessStatus) EnumDescriptor() ([]byte, []int) { return fileDescriptorLiveness, []int{0} }

// Liveness holds information about a node's latest heartbeat and epoch.
//
// NOTE: Care must be taken when changing the encoding of this proto
// because it is used as part of conditional put operations.
type Liveness struct {
	NodeID github_com_cockroachdb_cockroach_pkg_roachpb.NodeID `protobuf:"varint,1,opt,name=node_id,json=nodeId,proto3,casttype=github.com/cockroachdb/cockroach/pkg/roachpb.NodeID" json:"node_id,omitempty"`
	// Epoch is a monotonically-increasing value for node liveness. It
	// may be incremented if the liveness record expires (current time
	// is later than the expiration timestamp).
	Epoch int64 `protobuf:"varint,2,opt,name=epoch,proto3" json:"epoch,omitempty"`
	// The timestamp at which this liveness record expires.
	Expiration           cockroach_util_hlc1.LegacyTimestamp `protobuf:"bytes,3,opt,name=expiration" json:"expiration"`
	Draining             bool                                `protobuf:"varint,4,opt,name=draining,proto3" json:"draining,omitempty"`
	Decommissioning      bool                                `protobuf:"varint,5,opt,name=decommissioning,proto3" json:"decommissioning,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                            `json:"-"`
	XXX_sizecache        int32                               `json:"-"`
}

func (m *Liveness) Reset()                    { *m = Liveness{} }
func (m *Liveness) String() string            { return proto.CompactTextString(m) }
func (*Liveness) ProtoMessage()               {}
func (*Liveness) Descriptor() ([]byte, []int) { return fileDescriptorLiveness, []int{0} }
func (dst *Liveness) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Liveness.Merge(dst, src)
}
func (m *Liveness) XXX_Size() int {
	return xxx_messageInfo_Liveness.Size(m)
}
func (m *Liveness) XXX_DiscardUnknown() {
	xxx_messageInfo_Liveness.DiscardUnknown(m)
}

var xxx_messageInfo_Liveness proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Liveness)(nil), "cockroach.storage.Liveness")
	proto.RegisterEnum("cockroach.storage.NodeLivenessStatus", NodeLivenessStatus_name, NodeLivenessStatus_value)
}
func (m *Liveness) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Liveness) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.NodeID != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintLiveness(dAtA, i, uint64(m.NodeID))
	}
	if m.Epoch != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintLiveness(dAtA, i, uint64(m.Epoch))
	}
	dAtA[i] = 0x1a
	i++
	i = encodeVarintLiveness(dAtA, i, uint64(m.Expiration.Size()))
	n1, err := m.Expiration.MarshalTo(dAtA[i:])
	if err != nil {
		return 0, err
	}
	i += n1
	if m.Draining {
		dAtA[i] = 0x20
		i++
		if m.Draining {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i++
	}
	if m.Decommissioning {
		dAtA[i] = 0x28
		i++
		if m.Decommissioning {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i++
	}
	return i, nil
}

func encodeVarintLiveness(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func NewPopulatedLiveness(r randyLiveness, easy bool) *Liveness {
	this := &Liveness{}
	this.NodeID = github_com_cockroachdb_cockroach_pkg_roachpb.NodeID(r.Int31())
	if r.Intn(2) == 0 {
		this.NodeID *= -1
	}
	this.Epoch = int64(r.Int63())
	if r.Intn(2) == 0 {
		this.Epoch *= -1
	}
	v1 := cockroach_util_hlc1.NewPopulatedLegacyTimestamp(r, easy)
	this.Expiration = *v1
	this.Draining = bool(bool(r.Intn(2) == 0))
	this.Decommissioning = bool(bool(r.Intn(2) == 0))
	if !easy && r.Intn(10) != 0 {
	}
	return this
}

type randyLiveness interface {
	Float32() float32
	Float64() float64
	Int63() int64
	Int31() int32
	Uint32() uint32
	Intn(n int) int
}

func randUTF8RuneLiveness(r randyLiveness) rune {
	ru := r.Intn(62)
	if ru < 10 {
		return rune(ru + 48)
	} else if ru < 36 {
		return rune(ru + 55)
	}
	return rune(ru + 61)
}
func randStringLiveness(r randyLiveness) string {
	v2 := r.Intn(100)
	tmps := make([]rune, v2)
	for i := 0; i < v2; i++ {
		tmps[i] = randUTF8RuneLiveness(r)
	}
	return string(tmps)
}
func randUnrecognizedLiveness(r randyLiveness, maxFieldNumber int) (dAtA []byte) {
	l := r.Intn(5)
	for i := 0; i < l; i++ {
		wire := r.Intn(4)
		if wire == 3 {
			wire = 5
		}
		fieldNumber := maxFieldNumber + r.Intn(100)
		dAtA = randFieldLiveness(dAtA, r, fieldNumber, wire)
	}
	return dAtA
}
func randFieldLiveness(dAtA []byte, r randyLiveness, fieldNumber int, wire int) []byte {
	key := uint32(fieldNumber)<<3 | uint32(wire)
	switch wire {
	case 0:
		dAtA = encodeVarintPopulateLiveness(dAtA, uint64(key))
		v3 := r.Int63()
		if r.Intn(2) == 0 {
			v3 *= -1
		}
		dAtA = encodeVarintPopulateLiveness(dAtA, uint64(v3))
	case 1:
		dAtA = encodeVarintPopulateLiveness(dAtA, uint64(key))
		dAtA = append(dAtA, byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)))
	case 2:
		dAtA = encodeVarintPopulateLiveness(dAtA, uint64(key))
		ll := r.Intn(100)
		dAtA = encodeVarintPopulateLiveness(dAtA, uint64(ll))
		for j := 0; j < ll; j++ {
			dAtA = append(dAtA, byte(r.Intn(256)))
		}
	default:
		dAtA = encodeVarintPopulateLiveness(dAtA, uint64(key))
		dAtA = append(dAtA, byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)))
	}
	return dAtA
}
func encodeVarintPopulateLiveness(dAtA []byte, v uint64) []byte {
	for v >= 1<<7 {
		dAtA = append(dAtA, uint8(uint64(v)&0x7f|0x80))
		v >>= 7
	}
	dAtA = append(dAtA, uint8(v))
	return dAtA
}
func (m *Liveness) Size() (n int) {
	var l int
	_ = l
	if m.NodeID != 0 {
		n += 1 + sovLiveness(uint64(m.NodeID))
	}
	if m.Epoch != 0 {
		n += 1 + sovLiveness(uint64(m.Epoch))
	}
	l = m.Expiration.Size()
	n += 1 + l + sovLiveness(uint64(l))
	if m.Draining {
		n += 2
	}
	if m.Decommissioning {
		n += 2
	}
	return n
}

func sovLiveness(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozLiveness(x uint64) (n int) {
	return sovLiveness(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Liveness) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowLiveness
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
			return fmt.Errorf("proto: Liveness: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Liveness: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NodeID", wireType)
			}
			m.NodeID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLiveness
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.NodeID |= (github_com_cockroachdb_cockroach_pkg_roachpb.NodeID(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Epoch", wireType)
			}
			m.Epoch = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLiveness
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Epoch |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Expiration", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLiveness
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
				return ErrInvalidLengthLiveness
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.Expiration.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Draining", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLiveness
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Draining = bool(v != 0)
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Decommissioning", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLiveness
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Decommissioning = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipLiveness(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthLiveness
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
func skipLiveness(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowLiveness
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
					return 0, ErrIntOverflowLiveness
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
					return 0, ErrIntOverflowLiveness
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
				return 0, ErrInvalidLengthLiveness
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowLiveness
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
				next, err := skipLiveness(dAtA[start:])
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
	ErrInvalidLengthLiveness = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowLiveness   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("storage/liveness.proto", fileDescriptorLiveness) }

var fileDescriptorLiveness = []byte{
	// 413 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x5c, 0x51, 0xcd, 0x8e, 0x94, 0x40,
	0x18, 0x9c, 0x9e, 0x61, 0x7e, 0xd2, 0x93, 0x38, 0xd8, 0x6e, 0x0c, 0x99, 0x03, 0xa0, 0x5e, 0x88,
	0x07, 0x48, 0xdc, 0x9b, 0x37, 0x10, 0x62, 0x3a, 0xb2, 0x4c, 0xc2, 0xb8, 0x6b, 0xb2, 0x97, 0x0d,
	0x34, 0x1d, 0xe8, 0x2c, 0xd0, 0x04, 0x7a, 0x8c, 0xbe, 0x85, 0x89, 0x2f, 0xe0, 0x63, 0xf8, 0x08,
	0x73, 0xf4, 0xe8, 0x69, 0xa2, 0xf8, 0x16, 0x9e, 0x0c, 0x30, 0xbb, 0x3b, 0xf1, 0x56, 0x5f, 0x75,
	0xd5, 0x97, 0xaf, 0xba, 0xe0, 0xd3, 0x46, 0xf0, 0x3a, 0x4a, 0xa9, 0x95, 0xb3, 0x8f, 0xb4, 0xa4,
	0x4d, 0x63, 0x56, 0x35, 0x17, 0x1c, 0x3d, 0x26, 0x9c, 0xdc, 0xd6, 0x3c, 0x22, 0x99, 0x79, 0x54,
	0xac, 0xb5, 0x9d, 0x60, 0xb9, 0x95, 0xe5, 0xc4, 0xca, 0x69, 0x1a, 0x91, 0xcf, 0x37, 0x82, 0x15,
	0xb4, 0x11, 0x51, 0x51, 0x0d, 0x9e, 0xf5, 0x59, 0xca, 0x53, 0xde, 0x43, 0xab, 0x43, 0x03, 0xfb,
	0xfc, 0xeb, 0x18, 0x2e, 0xfc, 0xe3, 0x72, 0x74, 0x0d, 0xe7, 0x25, 0x4f, 0xe8, 0x0d, 0x4b, 0x14,
	0xa0, 0x03, 0x63, 0xea, 0xd8, 0xed, 0x41, 0x9b, 0x05, 0x3c, 0xa1, 0xd8, 0xfd, 0x7b, 0xd0, 0xce,
	0x53, 0x26, 0xb2, 0x5d, 0x6c, 0x12, 0x5e, 0x58, 0xf7, 0x07, 0x24, 0xf1, 0x03, 0xb6, 0xaa, 0xdb,
	0xd4, 0xea, 0x51, 0x15, 0x9b, 0x83, 0x2d, 0x9c, 0x75, 0x1b, 0x71, 0x82, 0xce, 0xe0, 0x94, 0x56,
	0x9c, 0x64, 0xca, 0x58, 0x07, 0xc6, 0x24, 0x1c, 0x06, 0x84, 0x21, 0xa4, 0x9f, 0x2a, 0x56, 0x47,
	0x82, 0xf1, 0x52, 0x99, 0xe8, 0xc0, 0x58, 0xbe, 0x7a, 0x61, 0x3e, 0xa4, 0xeb, 0x42, 0x99, 0x59,
	0x4e, 0x4c, 0xbf, 0x0f, 0xf5, 0xfe, 0x2e, 0x93, 0x23, 0xed, 0x0f, 0xda, 0x28, 0x3c, 0x31, 0xa3,
	0x35, 0x5c, 0x24, 0x75, 0xc4, 0x4a, 0x56, 0xa6, 0x8a, 0xa4, 0x03, 0x63, 0x11, 0xde, 0xcf, 0xc8,
	0x80, 0xab, 0x84, 0x12, 0x5e, 0x14, 0xac, 0x69, 0x18, 0xef, 0x25, 0xd3, 0x5e, 0xf2, 0x3f, 0xfd,
	0x5a, 0xfa, 0xfe, 0x4d, 0x03, 0x2f, 0x39, 0x44, 0xdd, 0xf9, 0x77, 0x1f, 0xb3, 0x15, 0x91, 0xd8,
	0x35, 0x68, 0x09, 0xe7, 0x97, 0xc1, 0xbb, 0x60, 0xf3, 0x21, 0x90, 0x47, 0x68, 0x01, 0x25, 0xd7,
	0xb3, 0x5d, 0x19, 0xa0, 0x15, 0x5c, 0x5e, 0x06, 0xf6, 0x95, 0x8d, 0x7d, 0xdb, 0xf1, 0x3d, 0x79,
	0xdc, 0x3d, 0xf9, 0xf8, 0xca, 0x93, 0x27, 0xe8, 0x09, 0x5c, 0xb9, 0xde, 0x9b, 0xcd, 0xc5, 0x05,
	0xde, 0x6e, 0xf1, 0x26, 0xc0, 0xc1, 0x5b, 0x59, 0x42, 0x08, 0x3e, 0x3a, 0x25, 0x3d, 0x57, 0x9e,
	0x3a, 0xcf, 0xf6, 0xbf, 0xd5, 0xd1, 0xbe, 0x55, 0xc1, 0x8f, 0x56, 0x05, 0x3f, 0x5b, 0x15, 0xfc,
	0x6a, 0x55, 0xf0, 0xe5, 0x8f, 0x3a, 0xba, 0x9e, 0x1f, 0x0b, 0x8e, 0x67, 0x7d, 0x61, 0xe7, 0xff,
	0x02, 0x00, 0x00, 0xff, 0xff, 0xaf, 0x43, 0xb9, 0x04, 0x14, 0x02, 0x00, 0x00,
}
