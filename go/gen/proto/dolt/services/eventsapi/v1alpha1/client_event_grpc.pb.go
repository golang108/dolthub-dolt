// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.4
// source: dolt/services/eventsapi/v1alpha1/client_event.proto

package eventsapi

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ClientEventsServiceClient is the client API for ClientEventsService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ClientEventsServiceClient interface {
	LogEvents(ctx context.Context, in *LogEventsRequest, opts ...grpc.CallOption) (*LogEventsResponse, error)
}

type clientEventsServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewClientEventsServiceClient(cc grpc.ClientConnInterface) ClientEventsServiceClient {
	return &clientEventsServiceClient{cc}
}

func (c *clientEventsServiceClient) LogEvents(ctx context.Context, in *LogEventsRequest, opts ...grpc.CallOption) (*LogEventsResponse, error) {
	out := new(LogEventsResponse)
	err := c.cc.Invoke(ctx, "/dolt.services.eventsapi.v1alpha1.ClientEventsService/LogEvents", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClientEventsServiceServer is the server API for ClientEventsService service.
// All implementations must embed UnimplementedClientEventsServiceServer
// for forward compatibility
type ClientEventsServiceServer interface {
	LogEvents(context.Context, *LogEventsRequest) (*LogEventsResponse, error)
	mustEmbedUnimplementedClientEventsServiceServer()
}

// UnimplementedClientEventsServiceServer must be embedded to have forward compatible implementations.
type UnimplementedClientEventsServiceServer struct {
}

func (UnimplementedClientEventsServiceServer) LogEvents(context.Context, *LogEventsRequest) (*LogEventsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LogEvents not implemented")
}
func (UnimplementedClientEventsServiceServer) mustEmbedUnimplementedClientEventsServiceServer() {}

// UnsafeClientEventsServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ClientEventsServiceServer will
// result in compilation errors.
type UnsafeClientEventsServiceServer interface {
	mustEmbedUnimplementedClientEventsServiceServer()
}

func RegisterClientEventsServiceServer(s grpc.ServiceRegistrar, srv ClientEventsServiceServer) {
	s.RegisterService(&ClientEventsService_ServiceDesc, srv)
}

func _ClientEventsService_LogEvents_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LogEventsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClientEventsServiceServer).LogEvents(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/dolt.services.eventsapi.v1alpha1.ClientEventsService/LogEvents",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClientEventsServiceServer).LogEvents(ctx, req.(*LogEventsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ClientEventsService_ServiceDesc is the grpc.ServiceDesc for ClientEventsService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ClientEventsService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "dolt.services.eventsapi.v1alpha1.ClientEventsService",
	HandlerType: (*ClientEventsServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "LogEvents",
			Handler:    _ClientEventsService_LogEvents_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "dolt/services/eventsapi/v1alpha1/client_event.proto",
}
