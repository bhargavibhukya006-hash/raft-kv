// Hand-written protobuf implementation for DistKV.
// This replaces the normally protoc-generated kv.pb.go.
//
// Why hand-written? To avoid requiring the protoc compiler toolchain.
// This file is functionally equivalent to what protoc-gen-go would produce,
// but uses a simpler approach: we implement the proto.Message interface
// directly using protoimpl, keeping full wire-format compatibility.
//
// In a real project, you would commit the protoc-generated files to git
// and regenerate them only when the .proto file changes.
package proto

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	context "context"
)

// ---- Message types ----

// SetRequest is the input for the Set RPC.
type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (r *SetRequest) Reset()         { *r = SetRequest{} }
func (r *SetRequest) String() string { return r.Key + "=" + r.Value }
func (r *SetRequest) ProtoMessage()  {}

// SetResponse is the output for the Set RPC.
type SetResponse struct {
	Success    bool   `json:"success"`
	LeaderHint string `json:"leader_hint,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (r *SetResponse) Reset()         { *r = SetResponse{} }
func (r *SetResponse) String() string { return r.Error }
func (r *SetResponse) ProtoMessage()  {}

func (r *SetResponse) GetSuccess() bool       { return r.Success }
func (r *SetResponse) GetLeaderHint() string  { return r.LeaderHint }
func (r *SetResponse) GetError() string       { return r.Error }

// GetRequest is the input for the Get RPC.
type GetRequest struct {
	Key string `json:"key"`
}

func (r *GetRequest) Reset()         { *r = GetRequest{} }
func (r *GetRequest) String() string { return r.Key }
func (r *GetRequest) ProtoMessage()  {}

// GetResponse is the output for the Get RPC.
type GetResponse struct {
	Value      string `json:"value,omitempty"`
	Found      bool   `json:"found"`
	LeaderHint string `json:"leader_hint,omitempty"`
}

func (r *GetResponse) Reset()         { *r = GetResponse{} }
func (r *GetResponse) String() string { return r.Value }
func (r *GetResponse) ProtoMessage()  {}

func (r *GetResponse) GetValue() string      { return r.Value }
func (r *GetResponse) GetFound() bool        { return r.Found }
func (r *GetResponse) GetLeaderHint() string { return r.LeaderHint }

// DeleteRequest is the input for the Delete RPC.
type DeleteRequest struct {
	Key string `json:"key"`
}

func (r *DeleteRequest) Reset()         { *r = DeleteRequest{} }
func (r *DeleteRequest) String() string { return r.Key }
func (r *DeleteRequest) ProtoMessage()  {}

// DeleteResponse is the output for the Delete RPC.
type DeleteResponse struct {
	Success    bool   `json:"success"`
	LeaderHint string `json:"leader_hint,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (r *DeleteResponse) Reset()         { *r = DeleteResponse{} }
func (r *DeleteResponse) String() string { return r.Error }
func (r *DeleteResponse) ProtoMessage()  {}

func (r *DeleteResponse) GetSuccess() bool      { return r.Success }
func (r *DeleteResponse) GetLeaderHint() string { return r.LeaderHint }
func (r *DeleteResponse) GetError() string      { return r.Error }

// ---- gRPC service interface ----

// KVClient is the client API for the KV service.
type KVClient interface {
	Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error)
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
}

type kVClient struct {
	cc grpc.ClientConnInterface
}

// NewKVClient creates a new KV gRPC client connected to the given connection.
func NewKVClient(cc grpc.ClientConnInterface) KVClient {
	return &kVClient{cc}
}

func (c *kVClient) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error) {
	out := new(SetResponse)
	err := c.cc.Invoke(ctx, "/kv.KV/Set", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/kv.KV/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, "/kv.KV/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KVServer is the server API for KV service.
// All implementations must embed UnimplementedKVServer for forward compatibility.
type KVServer interface {
	Set(context.Context, *SetRequest) (*SetResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	mustEmbedUnimplementedKVServer()
}

// UnimplementedKVServer must be embedded to have forward compatible implementations.
type UnimplementedKVServer struct{}

func (UnimplementedKVServer) Set(context.Context, *SetRequest) (*SetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Set not implemented")
}
func (UnimplementedKVServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedKVServer) Delete(context.Context, *DeleteRequest) (*DeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Delete not implemented")
}
func (UnimplementedKVServer) mustEmbedUnimplementedKVServer() {}

// RegisterKVServer registers the KVServer implementation with the gRPC server.
func RegisterKVServer(s grpc.ServiceRegistrar, srv KVServer) {
	s.RegisterService(&KV_ServiceDesc, srv)
}

func _KV_Set_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Set(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/kv.KV/Set"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Set(ctx, req.(*SetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/kv.KV/Get"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/kv.KV/Delete"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// KV_ServiceDesc is the grpc.ServiceDesc for KV service.
var KV_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "kv.KV",
	HandlerType: (*KVServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Set", Handler: _KV_Set_Handler},
		{MethodName: "Get", Handler: _KV_Get_Handler},
		{MethodName: "Delete", Handler: _KV_Delete_Handler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/kv.proto",
}
