# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: worker/worker_type.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from grpc_cluster.common import common_type_pb2 as grpc__cluster_dot_common_dot_common__type__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='worker/worker_type.proto',
  package='grpc_cluster.worker',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n\x18worker/worker_type.proto\x12\x13grpc_cluster.worker\x1a%grpc_cluster/common/common_type.proto\"M\n\x12GetMessagesRequest\x12)\n\x05token\x18\x01 \x01(\x0b\x32\x1a.grpc_cluster.common.Token\x12\x0c\n\x04tags\x18\x02 \x03(\t\"\x80\x01\n\x13GetMessagesResponse\x12\x0f\n\x07results\x18\x01 \x03(\t\x12+\n\x06status\x18\x02 \x01(\x0e\x32\x1b.grpc_cluster.common.Status\x12+\n\x05\x65rror\x18\x03 \x01(\x0b\x32\x1c.grpc_cluster.common.ErrCode\"<\n\x0fShutdownRequest\x12)\n\x05token\x18\x02 \x01(\x0b\x32\x1a.grpc_cluster.common.Tokenb\x06proto3')
  ,
  dependencies=[grpc__cluster_dot_common_dot_common__type__pb2.DESCRIPTOR,])




_GETMESSAGESREQUEST = _descriptor.Descriptor(
  name='GetMessagesRequest',
  full_name='grpc_cluster.worker.GetMessagesRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='token', full_name='grpc_cluster.worker.GetMessagesRequest.token', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='tags', full_name='grpc_cluster.worker.GetMessagesRequest.tags', index=1,
      number=2, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=88,
  serialized_end=165,
)


_GETMESSAGESRESPONSE = _descriptor.Descriptor(
  name='GetMessagesResponse',
  full_name='grpc_cluster.worker.GetMessagesResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='results', full_name='grpc_cluster.worker.GetMessagesResponse.results', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='status', full_name='grpc_cluster.worker.GetMessagesResponse.status', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='error', full_name='grpc_cluster.worker.GetMessagesResponse.error', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=168,
  serialized_end=296,
)


_SHUTDOWNREQUEST = _descriptor.Descriptor(
  name='ShutdownRequest',
  full_name='grpc_cluster.worker.ShutdownRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='token', full_name='grpc_cluster.worker.ShutdownRequest.token', index=0,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=298,
  serialized_end=358,
)

_GETMESSAGESREQUEST.fields_by_name['token'].message_type = grpc__cluster_dot_common_dot_common__type__pb2._TOKEN
_GETMESSAGESRESPONSE.fields_by_name['status'].enum_type = grpc__cluster_dot_common_dot_common__type__pb2._STATUS
_GETMESSAGESRESPONSE.fields_by_name['error'].message_type = grpc__cluster_dot_common_dot_common__type__pb2._ERRCODE
_SHUTDOWNREQUEST.fields_by_name['token'].message_type = grpc__cluster_dot_common_dot_common__type__pb2._TOKEN
DESCRIPTOR.message_types_by_name['GetMessagesRequest'] = _GETMESSAGESREQUEST
DESCRIPTOR.message_types_by_name['GetMessagesResponse'] = _GETMESSAGESRESPONSE
DESCRIPTOR.message_types_by_name['ShutdownRequest'] = _SHUTDOWNREQUEST
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

GetMessagesRequest = _reflection.GeneratedProtocolMessageType('GetMessagesRequest', (_message.Message,), dict(
  DESCRIPTOR = _GETMESSAGESREQUEST,
  __module__ = 'worker.worker_type_pb2'
  # @@protoc_insertion_point(class_scope:grpc_cluster.worker.GetMessagesRequest)
  ))
_sym_db.RegisterMessage(GetMessagesRequest)

GetMessagesResponse = _reflection.GeneratedProtocolMessageType('GetMessagesResponse', (_message.Message,), dict(
  DESCRIPTOR = _GETMESSAGESRESPONSE,
  __module__ = 'worker.worker_type_pb2'
  # @@protoc_insertion_point(class_scope:grpc_cluster.worker.GetMessagesResponse)
  ))
_sym_db.RegisterMessage(GetMessagesResponse)

ShutdownRequest = _reflection.GeneratedProtocolMessageType('ShutdownRequest', (_message.Message,), dict(
  DESCRIPTOR = _SHUTDOWNREQUEST,
  __module__ = 'worker.worker_type_pb2'
  # @@protoc_insertion_point(class_scope:grpc_cluster.worker.ShutdownRequest)
  ))
_sym_db.RegisterMessage(ShutdownRequest)


# @@protoc_insertion_point(module_scope)
