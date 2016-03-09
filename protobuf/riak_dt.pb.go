// Code generated by protoc-gen-go.
// source: riak_dt.proto
// DO NOT EDIT!

/*
Package protobuf is a generated protocol buffer package.


* Revision: 2.0

It is generated from these files:
	riak_dt.proto
	riak_kv.proto
	riak.proto
	riak_search.proto
	riak_yokozuna.proto

It has these top-level messages:
	MapField
	MapEntry
	DtFetchReq
	DtValue
	DtFetchResp
	CounterOp
	SetOp
	MapUpdate
	MapOp
	DtOp
	DtUpdateReq
	DtUpdateResp
	RpbGetClientIdResp
	RpbSetClientIdReq
	RpbGetReq
	RpbGetResp
	RpbPutReq
	RpbPutResp
	RpbDelReq
	RpbListBucketsReq
	RpbListBucketsResp
	RpbListKeysReq
	RpbListKeysResp
	RpbMapRedReq
	RpbMapRedResp
	RpbIndexReq
	RpbIndexResp
	RpbCSBucketReq
	RpbCSBucketResp
	RpbIndexObject
	RpbContent
	RpbLink
	RpbCounterUpdateReq
	RpbCounterUpdateResp
	RpbCounterGetReq
	RpbCounterGetResp
	RpbGetBucketKeyPreflistReq
	RpbGetBucketKeyPreflistResp
	RpbBucketKeyPreflistItem
	RpbErrorResp
	RpbGetServerInfoResp
	RpbPair
	RpbGetBucketReq
	RpbGetBucketResp
	RpbSetBucketReq
	RpbResetBucketReq
	RpbGetBucketTypeReq
	RpbSetBucketTypeReq
	RpbModFun
	RpbCommitHook
	RpbBucketProps
	RpbAuthReq
	RpbSearchDoc
	RpbSearchQueryReq
	RpbSearchQueryResp
	RpbYokozunaIndex
	RpbYokozunaIndexGetReq
	RpbYokozunaIndexGetResp
	RpbYokozunaIndexPutReq
	RpbYokozunaIndexDeleteReq
	RpbYokozunaSchema
	RpbYokozunaSchemaPutReq
	RpbYokozunaSchemaGetReq
	RpbYokozunaSchemaGetResp
*/
package protobuf

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
const _ = proto.ProtoPackageIsVersion1

//
// The types that can be stored in a map are limited to counters,
// sets, registers, flags, and maps.
type MapField_MapFieldType int32

const (
	MapField_COUNTER  MapField_MapFieldType = 1
	MapField_SET      MapField_MapFieldType = 2
	MapField_REGISTER MapField_MapFieldType = 3
	MapField_FLAG     MapField_MapFieldType = 4
	MapField_MAP      MapField_MapFieldType = 5
)

var MapField_MapFieldType_name = map[int32]string{
	1: "COUNTER",
	2: "SET",
	3: "REGISTER",
	4: "FLAG",
	5: "MAP",
}
var MapField_MapFieldType_value = map[string]int32{
	"COUNTER":  1,
	"SET":      2,
	"REGISTER": 3,
	"FLAG":     4,
	"MAP":      5,
}

func (x MapField_MapFieldType) Enum() *MapField_MapFieldType {
	p := new(MapField_MapFieldType)
	*p = x
	return p
}
func (x MapField_MapFieldType) String() string {
	return proto.EnumName(MapField_MapFieldType_name, int32(x))
}
func (x *MapField_MapFieldType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(MapField_MapFieldType_value, data, "MapField_MapFieldType")
	if err != nil {
		return err
	}
	*x = MapField_MapFieldType(value)
	return nil
}
func (MapField_MapFieldType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0, 0} }

type DtFetchResp_DataType int32

const (
	DtFetchResp_COUNTER DtFetchResp_DataType = 1
	DtFetchResp_SET     DtFetchResp_DataType = 2
	DtFetchResp_MAP     DtFetchResp_DataType = 3
)

var DtFetchResp_DataType_name = map[int32]string{
	1: "COUNTER",
	2: "SET",
	3: "MAP",
}
var DtFetchResp_DataType_value = map[string]int32{
	"COUNTER": 1,
	"SET":     2,
	"MAP":     3,
}

func (x DtFetchResp_DataType) Enum() *DtFetchResp_DataType {
	p := new(DtFetchResp_DataType)
	*p = x
	return p
}
func (x DtFetchResp_DataType) String() string {
	return proto.EnumName(DtFetchResp_DataType_name, int32(x))
}
func (x *DtFetchResp_DataType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(DtFetchResp_DataType_value, data, "DtFetchResp_DataType")
	if err != nil {
		return err
	}
	*x = DtFetchResp_DataType(value)
	return nil
}
func (DtFetchResp_DataType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{4, 0} }

//
// Flags only exist inside Maps and can only be enabled or
// disabled, and there are no arguments to the operations.
type MapUpdate_FlagOp int32

const (
	MapUpdate_ENABLE  MapUpdate_FlagOp = 1
	MapUpdate_DISABLE MapUpdate_FlagOp = 2
)

var MapUpdate_FlagOp_name = map[int32]string{
	1: "ENABLE",
	2: "DISABLE",
}
var MapUpdate_FlagOp_value = map[string]int32{
	"ENABLE":  1,
	"DISABLE": 2,
}

func (x MapUpdate_FlagOp) Enum() *MapUpdate_FlagOp {
	p := new(MapUpdate_FlagOp)
	*p = x
	return p
}
func (x MapUpdate_FlagOp) String() string {
	return proto.EnumName(MapUpdate_FlagOp_name, int32(x))
}
func (x *MapUpdate_FlagOp) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(MapUpdate_FlagOp_value, data, "MapUpdate_FlagOp")
	if err != nil {
		return err
	}
	*x = MapUpdate_FlagOp(value)
	return nil
}
func (MapUpdate_FlagOp) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{7, 0} }

//
// Field names in maps are composed of a binary identifier and a type.
// This is so that two clients can create fields with the same name
// but different types, and they converge independently.
type MapField struct {
	Name             []byte                 `protobuf:"bytes,1,req,name=name" json:"name,omitempty"`
	Type             *MapField_MapFieldType `protobuf:"varint,2,req,name=type,enum=protobuf.MapField_MapFieldType" json:"type,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *MapField) Reset()                    { *m = MapField{} }
func (m *MapField) String() string            { return proto.CompactTextString(m) }
func (*MapField) ProtoMessage()               {}
func (*MapField) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *MapField) GetName() []byte {
	if m != nil {
		return m.Name
	}
	return nil
}

func (m *MapField) GetType() MapField_MapFieldType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return MapField_COUNTER
}

//
// An entry in a map is a pair of a field-name and value. The type
// defined in the field determines which value type is expected.
type MapEntry struct {
	Field            *MapField   `protobuf:"bytes,1,req,name=field" json:"field,omitempty"`
	CounterValue     *int64      `protobuf:"zigzag64,2,opt,name=counter_value" json:"counter_value,omitempty"`
	SetValue         [][]byte    `protobuf:"bytes,3,rep,name=set_value" json:"set_value,omitempty"`
	RegisterValue    []byte      `protobuf:"bytes,4,opt,name=register_value" json:"register_value,omitempty"`
	FlagValue        *bool       `protobuf:"varint,5,opt,name=flag_value" json:"flag_value,omitempty"`
	MapValue         []*MapEntry `protobuf:"bytes,6,rep,name=map_value" json:"map_value,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *MapEntry) Reset()                    { *m = MapEntry{} }
func (m *MapEntry) String() string            { return proto.CompactTextString(m) }
func (*MapEntry) ProtoMessage()               {}
func (*MapEntry) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *MapEntry) GetField() *MapField {
	if m != nil {
		return m.Field
	}
	return nil
}

func (m *MapEntry) GetCounterValue() int64 {
	if m != nil && m.CounterValue != nil {
		return *m.CounterValue
	}
	return 0
}

func (m *MapEntry) GetSetValue() [][]byte {
	if m != nil {
		return m.SetValue
	}
	return nil
}

func (m *MapEntry) GetRegisterValue() []byte {
	if m != nil {
		return m.RegisterValue
	}
	return nil
}

func (m *MapEntry) GetFlagValue() bool {
	if m != nil && m.FlagValue != nil {
		return *m.FlagValue
	}
	return false
}

func (m *MapEntry) GetMapValue() []*MapEntry {
	if m != nil {
		return m.MapValue
	}
	return nil
}

//
// The equivalent of KV's "RpbGetReq", results in a DtFetchResp. The
// request-time options are limited to ones that are relevant to
// structured data-types.
type DtFetchReq struct {
	// The identifier: bucket, key and bucket-type
	Bucket []byte `protobuf:"bytes,1,req,name=bucket" json:"bucket,omitempty"`
	Key    []byte `protobuf:"bytes,2,req,name=key" json:"key,omitempty"`
	Type   []byte `protobuf:"bytes,3,req,name=type" json:"type,omitempty"`
	// Request options
	R            *uint32 `protobuf:"varint,4,opt,name=r" json:"r,omitempty"`
	Pr           *uint32 `protobuf:"varint,5,opt,name=pr" json:"pr,omitempty"`
	BasicQuorum  *bool   `protobuf:"varint,6,opt,name=basic_quorum" json:"basic_quorum,omitempty"`
	NotfoundOk   *bool   `protobuf:"varint,7,opt,name=notfound_ok" json:"notfound_ok,omitempty"`
	Timeout      *uint32 `protobuf:"varint,8,opt,name=timeout" json:"timeout,omitempty"`
	SloppyQuorum *bool   `protobuf:"varint,9,opt,name=sloppy_quorum" json:"sloppy_quorum,omitempty"`
	NVal         *uint32 `protobuf:"varint,10,opt,name=n_val" json:"n_val,omitempty"`
	// For read-only requests or context-free operations, you can set
	// this to false to reduce the size of the response payload.
	IncludeContext   *bool  `protobuf:"varint,11,opt,name=include_context,def=1" json:"include_context,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *DtFetchReq) Reset()                    { *m = DtFetchReq{} }
func (m *DtFetchReq) String() string            { return proto.CompactTextString(m) }
func (*DtFetchReq) ProtoMessage()               {}
func (*DtFetchReq) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

const Default_DtFetchReq_IncludeContext bool = true

func (m *DtFetchReq) GetBucket() []byte {
	if m != nil {
		return m.Bucket
	}
	return nil
}

func (m *DtFetchReq) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *DtFetchReq) GetType() []byte {
	if m != nil {
		return m.Type
	}
	return nil
}

func (m *DtFetchReq) GetR() uint32 {
	if m != nil && m.R != nil {
		return *m.R
	}
	return 0
}

func (m *DtFetchReq) GetPr() uint32 {
	if m != nil && m.Pr != nil {
		return *m.Pr
	}
	return 0
}

func (m *DtFetchReq) GetBasicQuorum() bool {
	if m != nil && m.BasicQuorum != nil {
		return *m.BasicQuorum
	}
	return false
}

func (m *DtFetchReq) GetNotfoundOk() bool {
	if m != nil && m.NotfoundOk != nil {
		return *m.NotfoundOk
	}
	return false
}

func (m *DtFetchReq) GetTimeout() uint32 {
	if m != nil && m.Timeout != nil {
		return *m.Timeout
	}
	return 0
}

func (m *DtFetchReq) GetSloppyQuorum() bool {
	if m != nil && m.SloppyQuorum != nil {
		return *m.SloppyQuorum
	}
	return false
}

func (m *DtFetchReq) GetNVal() uint32 {
	if m != nil && m.NVal != nil {
		return *m.NVal
	}
	return 0
}

func (m *DtFetchReq) GetIncludeContext() bool {
	if m != nil && m.IncludeContext != nil {
		return *m.IncludeContext
	}
	return Default_DtFetchReq_IncludeContext
}

//
// The value of the fetched data type. If present in the response,
// then empty values (sets, maps) should be treated as such.
type DtValue struct {
	CounterValue     *int64      `protobuf:"zigzag64,1,opt,name=counter_value" json:"counter_value,omitempty"`
	SetValue         [][]byte    `protobuf:"bytes,2,rep,name=set_value" json:"set_value,omitempty"`
	MapValue         []*MapEntry `protobuf:"bytes,3,rep,name=map_value" json:"map_value,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *DtValue) Reset()                    { *m = DtValue{} }
func (m *DtValue) String() string            { return proto.CompactTextString(m) }
func (*DtValue) ProtoMessage()               {}
func (*DtValue) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *DtValue) GetCounterValue() int64 {
	if m != nil && m.CounterValue != nil {
		return *m.CounterValue
	}
	return 0
}

func (m *DtValue) GetSetValue() [][]byte {
	if m != nil {
		return m.SetValue
	}
	return nil
}

func (m *DtValue) GetMapValue() []*MapEntry {
	if m != nil {
		return m.MapValue
	}
	return nil
}

//
// The response to a "Fetch" request. If the `include_context` option
// is specified, an opaque "context" value will be returned along with
// the user-friendly data. When sending an "Update" request, the
// client should send this context as well, similar to how one would
// send a vclock for KV updates. The `type` field indicates which
// value type to expect. When the `value` field is missing from the
// message, the client should interpret it as a "not found".
type DtFetchResp struct {
	Context          []byte                `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
	Type             *DtFetchResp_DataType `protobuf:"varint,2,req,name=type,enum=protobuf.DtFetchResp_DataType" json:"type,omitempty"`
	Value            *DtValue              `protobuf:"bytes,3,opt,name=value" json:"value,omitempty"`
	XXX_unrecognized []byte                `json:"-"`
}

func (m *DtFetchResp) Reset()                    { *m = DtFetchResp{} }
func (m *DtFetchResp) String() string            { return proto.CompactTextString(m) }
func (*DtFetchResp) ProtoMessage()               {}
func (*DtFetchResp) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *DtFetchResp) GetContext() []byte {
	if m != nil {
		return m.Context
	}
	return nil
}

func (m *DtFetchResp) GetType() DtFetchResp_DataType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return DtFetchResp_COUNTER
}

func (m *DtFetchResp) GetValue() *DtValue {
	if m != nil {
		return m.Value
	}
	return nil
}

//
// An operation to update a Counter, either on its own or inside a
// Map. The `increment` field can be positive or negative. When absent,
// the meaning is an increment by 1.
type CounterOp struct {
	Increment        *int64 `protobuf:"zigzag64,1,opt,name=increment" json:"increment,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *CounterOp) Reset()                    { *m = CounterOp{} }
func (m *CounterOp) String() string            { return proto.CompactTextString(m) }
func (*CounterOp) ProtoMessage()               {}
func (*CounterOp) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *CounterOp) GetIncrement() int64 {
	if m != nil && m.Increment != nil {
		return *m.Increment
	}
	return 0
}

//
// An operation to update a Set, either on its own or inside a Map.
// Set members are opaque binary values, you can only add or remove
// them from a Set.
type SetOp struct {
	Adds             [][]byte `protobuf:"bytes,1,rep,name=adds" json:"adds,omitempty"`
	Removes          [][]byte `protobuf:"bytes,2,rep,name=removes" json:"removes,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *SetOp) Reset()                    { *m = SetOp{} }
func (m *SetOp) String() string            { return proto.CompactTextString(m) }
func (*SetOp) ProtoMessage()               {}
func (*SetOp) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

func (m *SetOp) GetAdds() [][]byte {
	if m != nil {
		return m.Adds
	}
	return nil
}

func (m *SetOp) GetRemoves() [][]byte {
	if m != nil {
		return m.Removes
	}
	return nil
}

//
// An operation to be applied to a value stored in a Map -- the
// contents of an UPDATE operation. The operation field that is
// present depends on the type of the field to which it is applied.
type MapUpdate struct {
	Field     *MapField  `protobuf:"bytes,1,req,name=field" json:"field,omitempty"`
	CounterOp *CounterOp `protobuf:"bytes,2,opt,name=counter_op" json:"counter_op,omitempty"`
	SetOp     *SetOp     `protobuf:"bytes,3,opt,name=set_op" json:"set_op,omitempty"`
	//
	// There is only one operation on a register, which is to set its
	// value, therefore the "operation" is the new value.
	RegisterOp       []byte            `protobuf:"bytes,4,opt,name=register_op" json:"register_op,omitempty"`
	FlagOp           *MapUpdate_FlagOp `protobuf:"varint,5,opt,name=flag_op,enum=protobuf.MapUpdate_FlagOp" json:"flag_op,omitempty"`
	MapOp            *MapOp            `protobuf:"bytes,6,opt,name=map_op" json:"map_op,omitempty"`
	XXX_unrecognized []byte            `json:"-"`
}

func (m *MapUpdate) Reset()                    { *m = MapUpdate{} }
func (m *MapUpdate) String() string            { return proto.CompactTextString(m) }
func (*MapUpdate) ProtoMessage()               {}
func (*MapUpdate) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *MapUpdate) GetField() *MapField {
	if m != nil {
		return m.Field
	}
	return nil
}

func (m *MapUpdate) GetCounterOp() *CounterOp {
	if m != nil {
		return m.CounterOp
	}
	return nil
}

func (m *MapUpdate) GetSetOp() *SetOp {
	if m != nil {
		return m.SetOp
	}
	return nil
}

func (m *MapUpdate) GetRegisterOp() []byte {
	if m != nil {
		return m.RegisterOp
	}
	return nil
}

func (m *MapUpdate) GetFlagOp() MapUpdate_FlagOp {
	if m != nil && m.FlagOp != nil {
		return *m.FlagOp
	}
	return MapUpdate_ENABLE
}

func (m *MapUpdate) GetMapOp() *MapOp {
	if m != nil {
		return m.MapOp
	}
	return nil
}

//
// An operation to update a Map. All operations apply to individual
// fields in the Map.
type MapOp struct {
	//
	//  REMOVE removes a field and value from the Map.
	// UPDATE applies type-specific
	// operations to the values stored in the Map.
	Removes          []*MapField  `protobuf:"bytes,1,rep,name=removes" json:"removes,omitempty"`
	Updates          []*MapUpdate `protobuf:"bytes,2,rep,name=updates" json:"updates,omitempty"`
	XXX_unrecognized []byte       `json:"-"`
}

func (m *MapOp) Reset()                    { *m = MapOp{} }
func (m *MapOp) String() string            { return proto.CompactTextString(m) }
func (*MapOp) ProtoMessage()               {}
func (*MapOp) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

func (m *MapOp) GetRemoves() []*MapField {
	if m != nil {
		return m.Removes
	}
	return nil
}

func (m *MapOp) GetUpdates() []*MapUpdate {
	if m != nil {
		return m.Updates
	}
	return nil
}

//
// A "union" type for update operations. The included operation
// depends on the datatype being updated.
type DtOp struct {
	CounterOp        *CounterOp `protobuf:"bytes,1,opt,name=counter_op" json:"counter_op,omitempty"`
	SetOp            *SetOp     `protobuf:"bytes,2,opt,name=set_op" json:"set_op,omitempty"`
	MapOp            *MapOp     `protobuf:"bytes,3,opt,name=map_op" json:"map_op,omitempty"`
	XXX_unrecognized []byte     `json:"-"`
}

func (m *DtOp) Reset()                    { *m = DtOp{} }
func (m *DtOp) String() string            { return proto.CompactTextString(m) }
func (*DtOp) ProtoMessage()               {}
func (*DtOp) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

func (m *DtOp) GetCounterOp() *CounterOp {
	if m != nil {
		return m.CounterOp
	}
	return nil
}

func (m *DtOp) GetSetOp() *SetOp {
	if m != nil {
		return m.SetOp
	}
	return nil
}

func (m *DtOp) GetMapOp() *MapOp {
	if m != nil {
		return m.MapOp
	}
	return nil
}

//
// The equivalent of KV's "RpbPutReq", results in an empty response or
// "DtUpdateResp" if `return_body` is specified, or the key is
// assigned by the server. The request-time options are limited to
// ones that are relevant to structured data-types.
type DtUpdateReq struct {
	// The identifier
	Bucket []byte `protobuf:"bytes,1,req,name=bucket" json:"bucket,omitempty"`
	Key    []byte `protobuf:"bytes,2,opt,name=key" json:"key,omitempty"`
	Type   []byte `protobuf:"bytes,3,req,name=type" json:"type,omitempty"`
	// Opaque update-context
	Context []byte `protobuf:"bytes,4,opt,name=context" json:"context,omitempty"`
	// The operations
	Op *DtOp `protobuf:"bytes,5,req,name=op" json:"op,omitempty"`
	// Request options
	W                *uint32 `protobuf:"varint,6,opt,name=w" json:"w,omitempty"`
	Dw               *uint32 `protobuf:"varint,7,opt,name=dw" json:"dw,omitempty"`
	Pw               *uint32 `protobuf:"varint,8,opt,name=pw" json:"pw,omitempty"`
	ReturnBody       *bool   `protobuf:"varint,9,opt,name=return_body,def=0" json:"return_body,omitempty"`
	Timeout          *uint32 `protobuf:"varint,10,opt,name=timeout" json:"timeout,omitempty"`
	SloppyQuorum     *bool   `protobuf:"varint,11,opt,name=sloppy_quorum" json:"sloppy_quorum,omitempty"`
	NVal             *uint32 `protobuf:"varint,12,opt,name=n_val" json:"n_val,omitempty"`
	IncludeContext   *bool   `protobuf:"varint,13,opt,name=include_context,def=1" json:"include_context,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *DtUpdateReq) Reset()                    { *m = DtUpdateReq{} }
func (m *DtUpdateReq) String() string            { return proto.CompactTextString(m) }
func (*DtUpdateReq) ProtoMessage()               {}
func (*DtUpdateReq) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

const Default_DtUpdateReq_ReturnBody bool = false
const Default_DtUpdateReq_IncludeContext bool = true

func (m *DtUpdateReq) GetBucket() []byte {
	if m != nil {
		return m.Bucket
	}
	return nil
}

func (m *DtUpdateReq) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *DtUpdateReq) GetType() []byte {
	if m != nil {
		return m.Type
	}
	return nil
}

func (m *DtUpdateReq) GetContext() []byte {
	if m != nil {
		return m.Context
	}
	return nil
}

func (m *DtUpdateReq) GetOp() *DtOp {
	if m != nil {
		return m.Op
	}
	return nil
}

func (m *DtUpdateReq) GetW() uint32 {
	if m != nil && m.W != nil {
		return *m.W
	}
	return 0
}

func (m *DtUpdateReq) GetDw() uint32 {
	if m != nil && m.Dw != nil {
		return *m.Dw
	}
	return 0
}

func (m *DtUpdateReq) GetPw() uint32 {
	if m != nil && m.Pw != nil {
		return *m.Pw
	}
	return 0
}

func (m *DtUpdateReq) GetReturnBody() bool {
	if m != nil && m.ReturnBody != nil {
		return *m.ReturnBody
	}
	return Default_DtUpdateReq_ReturnBody
}

func (m *DtUpdateReq) GetTimeout() uint32 {
	if m != nil && m.Timeout != nil {
		return *m.Timeout
	}
	return 0
}

func (m *DtUpdateReq) GetSloppyQuorum() bool {
	if m != nil && m.SloppyQuorum != nil {
		return *m.SloppyQuorum
	}
	return false
}

func (m *DtUpdateReq) GetNVal() uint32 {
	if m != nil && m.NVal != nil {
		return *m.NVal
	}
	return 0
}

func (m *DtUpdateReq) GetIncludeContext() bool {
	if m != nil && m.IncludeContext != nil {
		return *m.IncludeContext
	}
	return Default_DtUpdateReq_IncludeContext
}

//
// The equivalent of KV's "RpbPutResp", contains the assigned key if
// it was assigned by the server, and the resulting value and context
// if return_body was set.
type DtUpdateResp struct {
	// The key, if assigned by the server
	Key []byte `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	// The opaque update context and value, if return_body was set.
	Context          []byte      `protobuf:"bytes,2,opt,name=context" json:"context,omitempty"`
	CounterValue     *int64      `protobuf:"zigzag64,3,opt,name=counter_value" json:"counter_value,omitempty"`
	SetValue         [][]byte    `protobuf:"bytes,4,rep,name=set_value" json:"set_value,omitempty"`
	MapValue         []*MapEntry `protobuf:"bytes,5,rep,name=map_value" json:"map_value,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *DtUpdateResp) Reset()                    { *m = DtUpdateResp{} }
func (m *DtUpdateResp) String() string            { return proto.CompactTextString(m) }
func (*DtUpdateResp) ProtoMessage()               {}
func (*DtUpdateResp) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{11} }

func (m *DtUpdateResp) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *DtUpdateResp) GetContext() []byte {
	if m != nil {
		return m.Context
	}
	return nil
}

func (m *DtUpdateResp) GetCounterValue() int64 {
	if m != nil && m.CounterValue != nil {
		return *m.CounterValue
	}
	return 0
}

func (m *DtUpdateResp) GetSetValue() [][]byte {
	if m != nil {
		return m.SetValue
	}
	return nil
}

func (m *DtUpdateResp) GetMapValue() []*MapEntry {
	if m != nil {
		return m.MapValue
	}
	return nil
}

func init() {
	proto.RegisterType((*MapField)(nil), "protobuf.MapField")
	proto.RegisterType((*MapEntry)(nil), "protobuf.MapEntry")
	proto.RegisterType((*DtFetchReq)(nil), "protobuf.DtFetchReq")
	proto.RegisterType((*DtValue)(nil), "protobuf.DtValue")
	proto.RegisterType((*DtFetchResp)(nil), "protobuf.DtFetchResp")
	proto.RegisterType((*CounterOp)(nil), "protobuf.CounterOp")
	proto.RegisterType((*SetOp)(nil), "protobuf.SetOp")
	proto.RegisterType((*MapUpdate)(nil), "protobuf.MapUpdate")
	proto.RegisterType((*MapOp)(nil), "protobuf.MapOp")
	proto.RegisterType((*DtOp)(nil), "protobuf.DtOp")
	proto.RegisterType((*DtUpdateReq)(nil), "protobuf.DtUpdateReq")
	proto.RegisterType((*DtUpdateResp)(nil), "protobuf.DtUpdateResp")
	proto.RegisterEnum("protobuf.MapField_MapFieldType", MapField_MapFieldType_name, MapField_MapFieldType_value)
	proto.RegisterEnum("protobuf.DtFetchResp_DataType", DtFetchResp_DataType_name, DtFetchResp_DataType_value)
	proto.RegisterEnum("protobuf.MapUpdate_FlagOp", MapUpdate_FlagOp_name, MapUpdate_FlagOp_value)
}

var fileDescriptor0 = []byte{
	// 793 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x8c, 0x55, 0xcb, 0x6e, 0xe2, 0x48,
	0x14, 0x95, 0x5f, 0x60, 0xae, 0x0d, 0x21, 0x95, 0x79, 0xa0, 0x48, 0x93, 0x87, 0x33, 0x8f, 0x8c,
	0x66, 0x86, 0x05, 0xcb, 0xec, 0x42, 0x80, 0x28, 0x52, 0x5e, 0x82, 0x64, 0x16, 0xb3, 0x41, 0x06,
	0x17, 0x09, 0x02, 0x6c, 0xc7, 0x2e, 0x27, 0xc3, 0x62, 0x7e, 0x61, 0xf6, 0xfd, 0x0b, 0xfd, 0x33,
	0xfd, 0x3f, 0xbd, 0x68, 0xf5, 0xad, 0x2a, 0x83, 0x4d, 0x20, 0xad, 0xac, 0x92, 0xba, 0x39, 0xe5,
	0x7b, 0xce, 0xa9, 0x73, 0x6f, 0xa0, 0x1c, 0x8d, 0xdd, 0x49, 0xdf, 0x63, 0xf5, 0x30, 0x0a, 0x58,
	0x40, 0x4c, 0xf1, 0x63, 0x90, 0x8c, 0x9c, 0xff, 0x15, 0x30, 0xaf, 0xdc, 0xb0, 0x33, 0xa6, 0x53,
	0x8f, 0xd8, 0xa0, 0xfb, 0xee, 0x8c, 0xd6, 0x94, 0x03, 0xf5, 0xd8, 0x26, 0x7f, 0x81, 0xce, 0xe6,
	0x21, 0xad, 0xa9, 0x78, 0xaa, 0x34, 0xf6, 0xeb, 0x8b, 0x3b, 0xf5, 0x05, 0x7e, 0xf9, 0xcb, 0x1d,
	0xc2, 0x9c, 0x36, 0xd8, 0xf9, 0x33, 0xb1, 0xa0, 0x78, 0x76, 0x73, 0x7f, 0x7d, 0xd7, 0xee, 0x56,
	0x15, 0x52, 0x04, 0xad, 0xd7, 0xbe, 0xab, 0xaa, 0xd8, 0xc2, 0xec, 0xb6, 0xcf, 0x2f, 0x7a, 0xbc,
	0xac, 0x11, 0x13, 0xf4, 0xce, 0xe5, 0xe9, 0x79, 0x55, 0xe7, 0x80, 0xab, 0xd3, 0xdb, 0xaa, 0xe1,
	0x7c, 0x94, 0x84, 0xda, 0x3e, 0x8b, 0xe6, 0xe4, 0x10, 0x8c, 0x11, 0xff, 0xa0, 0x60, 0x64, 0x35,
	0xc8, 0x3a, 0x07, 0xf2, 0x3d, 0x94, 0x87, 0x41, 0xe2, 0x33, 0x1a, 0xf5, 0x9f, 0xdd, 0x69, 0xc2,
	0xe9, 0x2a, 0xc7, 0x84, 0x6c, 0x43, 0x29, 0xa6, 0x2c, 0x2d, 0x69, 0x07, 0x1a, 0xea, 0xf9, 0x01,
	0x2a, 0x11, 0x7d, 0x18, 0xc7, 0x19, 0x54, 0x47, 0xa8, 0x4d, 0x08, 0xc0, 0x68, 0xea, 0x3e, 0xa4,
	0x35, 0x03, 0x6b, 0x26, 0xf9, 0x05, 0x4a, 0x33, 0x37, 0x4c, 0x4b, 0x05, 0xbc, 0xfe, 0xba, 0xb9,
	0xe0, 0xe7, 0x7c, 0x52, 0x00, 0x5a, 0xac, 0x43, 0xd9, 0xf0, 0xb1, 0x4b, 0x9f, 0x48, 0x05, 0x0a,
	0x83, 0x64, 0x38, 0xa1, 0x2c, 0x75, 0xd0, 0x02, 0x6d, 0x42, 0xe7, 0xc2, 0x40, 0x9b, 0x9b, 0x2b,
	0xec, 0xd4, 0xc4, 0xa9, 0x04, 0x4a, 0x24, 0xfa, 0x97, 0xb1, 0xbd, 0x1a, 0x46, 0xa2, 0x6f, 0x99,
	0x7c, 0x07, 0xf6, 0xc0, 0x8d, 0xc7, 0xc3, 0xfe, 0x53, 0x12, 0x44, 0xc9, 0x0c, 0x5b, 0x73, 0x36,
	0x3b, 0x60, 0xf9, 0x01, 0x1b, 0xa1, 0x4c, 0xaf, 0x1f, 0x4c, 0x6a, 0x45, 0x51, 0xdc, 0x82, 0x22,
	0x1b, 0xcf, 0x68, 0x90, 0xb0, 0x9a, 0x29, 0xee, 0xa2, 0x13, 0xf1, 0x34, 0x08, 0xc3, 0xf9, 0xe2,
	0x72, 0x49, 0xe0, 0xca, 0x60, 0xf8, 0x5c, 0x48, 0x0d, 0x04, 0xea, 0x27, 0xd8, 0x1a, 0xfb, 0xc3,
	0x69, 0xe2, 0xd1, 0xfe, 0x30, 0x40, 0xdb, 0xfe, 0x65, 0x35, 0x8b, 0xe3, 0x4e, 0x74, 0x16, 0x25,
	0xd4, 0xf9, 0x07, 0x8a, 0x2d, 0xf6, 0x37, 0x97, 0xbd, 0xee, 0xac, 0xb2, 0xee, 0xac, 0x2a, 0x9c,
	0x5d, 0x71, 0x4b, 0x7b, 0xd3, 0xad, 0x0f, 0x0a, 0x58, 0x4b, 0xb7, 0xe2, 0x90, 0x2b, 0x58, 0x50,
	0x50, 0xc4, 0x4b, 0xfc, 0xb9, 0x92, 0xb8, 0xbd, 0xec, 0x13, 0xb9, 0x5b, 0xf5, 0x96, 0xcb, 0x5c,
	0x11, 0xb0, 0x03, 0x30, 0x16, 0x1d, 0x15, 0xec, 0xb8, 0x9d, 0x87, 0x0b, 0x05, 0xce, 0xef, 0x60,
	0x2e, 0xd1, 0x9b, 0xe3, 0x98, 0xc6, 0x4e, 0x73, 0xf6, 0xa0, 0x74, 0x26, 0xc5, 0xde, 0x84, 0x5c,
	0x22, 0x7a, 0x14, 0xd1, 0x19, 0xf5, 0x25, 0x35, 0xe2, 0xfc, 0x0a, 0x46, 0x8f, 0x32, 0xfc, 0x1b,
	0x3e, 0xa3, 0xeb, 0x79, 0x31, 0x96, 0xb9, 0x72, 0x94, 0x80, 0xb0, 0xe0, 0x99, 0xc6, 0xd2, 0x0a,
	0xe7, 0x8b, 0x02, 0x25, 0x14, 0x7c, 0x1f, 0x7a, 0x2e, 0xa3, 0xef, 0xc9, 0xef, 0x6f, 0x00, 0x0b,
	0x97, 0x83, 0x50, 0x84, 0xd7, 0x6a, 0xec, 0x64, 0xb8, 0x8c, 0xd4, 0x3e, 0x14, 0xb8, 0xef, 0x08,
	0x92, 0x7a, 0xb7, 0x32, 0x90, 0x64, 0x86, 0x29, 0x59, 0xe6, 0x1b, 0x51, 0x32, 0xdc, 0x7f, 0x40,
	0x51, 0x84, 0x1b, 0x0b, 0x3c, 0x61, 0x95, 0xc6, 0xee, 0x0a, 0x07, 0xc9, 0xb3, 0xde, 0x41, 0x88,
	0x6c, 0xc1, 0xdf, 0x11, 0xb1, 0x85, 0xd7, 0x2d, 0x10, 0x7b, 0x13, 0x3a, 0x87, 0x50, 0x48, 0xa1,
	0x00, 0x85, 0xf6, 0xf5, 0x69, 0xf3, 0xb2, 0x8d, 0x6e, 0xa2, 0xb5, 0xad, 0x8b, 0x9e, 0x38, 0xa8,
	0x4e, 0x17, 0x0c, 0x81, 0x25, 0x47, 0x99, 0x35, 0xca, 0x86, 0x48, 0x48, 0xf5, 0x3f, 0x43, 0x31,
	0x11, 0x14, 0xa4, 0x7f, 0x2b, 0xd2, 0x97, 0xf4, 0x9c, 0x27, 0xd0, 0x5b, 0x5c, 0xe1, 0xaa, 0x57,
	0xca, 0x7b, 0xbc, 0x52, 0x37, 0x7b, 0x95, 0x29, 0xd5, 0x36, 0x2b, 0xfd, 0x2c, 0xb2, 0x2a, 0xfb,
	0x7f, 0x73, 0xb4, 0x95, 0xb5, 0xd1, 0xce, 0xc5, 0x5a, 0xbe, 0xc1, 0x2e, 0xa8, 0xc2, 0x7e, 0x1e,
	0x81, 0x4a, 0x3e, 0xa5, 0x48, 0x04, 0xf7, 0xc0, 0x8b, 0x70, 0x5b, 0xec, 0x01, 0xef, 0x45, 0x0c,
	0xb7, 0xdc, 0x09, 0x2f, 0xe9, 0x5c, 0xef, 0xf2, 0x77, 0x65, 0x49, 0xe4, 0xf7, 0x07, 0x81, 0x37,
	0x97, 0x53, 0x7d, 0x62, 0x8c, 0xdc, 0x69, 0x4c, 0xf3, 0x4b, 0x00, 0x36, 0x2f, 0x01, 0x6b, 0x75,
	0x09, 0xd8, 0x6f, 0x2d, 0x81, 0x72, 0x6e, 0x09, 0xfc, 0x07, 0x76, 0xa6, 0x1d, 0x07, 0x35, 0x15,
	0x2b, 0x87, 0x34, 0x27, 0x4f, 0xaa, 0x5f, 0xdb, 0x13, 0xda, 0xfa, 0x9e, 0xd0, 0xd7, 0xf7, 0x84,
	0xf1, 0xd6, 0x9e, 0x68, 0x1e, 0xc1, 0x8f, 0xc3, 0x60, 0x56, 0xc7, 0x45, 0xf8, 0x18, 0xd4, 0xf9,
	0x3f, 0xae, 0x25, 0xa6, 0x69, 0x76, 0xf1, 0xd8, 0x62, 0xb7, 0xcd, 0xaf, 0x01, 0x00, 0x00, 0xff,
	0xff, 0x06, 0x2e, 0x6d, 0x82, 0xd2, 0x06, 0x00, 0x00,
}