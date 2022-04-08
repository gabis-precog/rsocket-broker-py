import abc
import struct
from abc import ABCMeta
from enum import IntEnum, unique
from typing import Optional

from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketProtocolError, ParseError, RSocketUnknownFrameType
from rsocket.frame_helpers import (is_flag_set, unpack_string, pack_string)

from rsocket_broker.frame_helpers import parse_key_value_map, serialize_key_value

PROTOCOL_MAJOR_VERSION = 0
PROTOCOL_MINOR_VERSION = 1


_FLAG_RESERVED_BIT = 0x200
_FLAG_ENCRYPTED_BIT = 0x100
_FLAG_UNICAST_BIT = 0x80
_FLAG_MULTICAST_BIT = 0x40
_FLAG_SHARED_ROUTING_BIT = 0x20


@unique
class FrameType(IntEnum):
    RESERVED = 0
    ROUTE_SETUP = 1
    ROUTE_ADD = 2
    ROUTE_REMOVE = 3
    BROKER_INFO = 4
    ADDRESS = 5


HEADER_LENGTH = 6  # A full header is 4 (stream) + 2 (type, flags) bytes.


class Header:
    __slots__ = (
        'length',
        'major_version',
        'minor_version',
        'frame_type',
        'flags_ignore',
        'flags_metadata'
    )


def parse_header(frame: Header, buffer: bytes, offset: int) -> int:
    frame.length = len(buffer)
    (frame.major_version, frame.minor_version, frame.frame_type, flags) = struct.unpack_from('>HHBB', buffer, offset)
    flags |= (frame.frame_type & 3) << 8
    frame_type_id = frame.frame_type >> 2
    frame.frame_type = FrameType(frame_type_id)
    return flags


class FragmentableFrame:
    __slots__ = (
        'metadata',
        'data',
        'flags_follows',
        'flags_complete',
        'flags_next',
        'stream_id'
    )


class Frame(Header, metaclass=ABCMeta):
    __slots__ = (
        'major_version',
        'minor_version',
        'frame_type',
        'flags'
    )

    def __init__(self, frame_type: FrameType):
        self.major_version = PROTOCOL_MAJOR_VERSION
        self.minor_version = PROTOCOL_MINOR_VERSION
        self.frame_type = frame_type

    @staticmethod
    def pack_string(buffer):
        return struct.pack('b', len(buffer)) + buffer

    @abc.abstractmethod
    def parse(self, buffer: bytes, offset: int):
        ...

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        buffer = bytearray(5)
        buffer[0:3] = struct.pack('>HH', self.major_version, self.minor_version)
        buffer[4] = (self.frame_type << 2) | (flags >> 8)
        buffer[5] = flags & 0xff

        return bytes(buffer) + middle


class RouteSetupFrame(Frame):
    __slots__ = (
        'route_id',
        'service_name',
        'key_value_map'
    )

    def __init__(self):
        super().__init__(FrameType.ROUTE_SETUP)

    def parse(self, buffer: bytes, offset: int):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH

        self.route_id = buffer[offset:offset + 16]
        offset += 16
        length, self.service_name = unpack_string(buffer, offset)
        offset += length + 1

        self.key_value_map, _ = parse_key_value_map(buffer, offset)

    def serialize(self, middle=b'', flags=0) -> bytes:
        middle += self.route_id

        middle += pack_string(self.service_name)

        middle += serialize_key_value(self.key_value_map)

        return Frame.serialize(self, middle)


class InvalidFrame:
    pass


class RouteAddFrame(Frame):
    __slots__ = (
        'broker_id',
        'route_id',
        'timestamp',
        'service_name',
        'key_value_map'
    )

    def __init__(self):
        super().__init__(FrameType.ROUTE_ADD)
        self.service_name = None
        self.key_value_map = {}
        self.route_id = None
        self.timestamp = None
        self.broker_id = None

    def parse(self, buffer: bytes, offset: int):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH

        self.broker_id = buffer[offset:offset + 16]
        offset += 16
        self.route_id = buffer[offset:offset + 16]
        offset += 16
        self.timestamp = struct.unpack('>Q', buffer[offset:offset + 8])[0]
        offset += 8
        length, self.service_name = unpack_string(buffer, offset)
        offset += length + 1

        self.key_value_map, _ = parse_key_value_map(buffer, offset)

    def serialize(self, middle=b'', flags=0) -> bytes:
        middle += self.broker_id
        middle += self.route_id
        middle += struct.pack('>Q', self.timestamp)
        middle += pack_string(self.service_name)

        middle += serialize_key_value(self.key_value_map)

        return Frame.serialize(self, middle)


class RouteRemoveFrame(Frame):
    __slots__ = (
        'broker_id',
        'route_id',
        'timestamp'
    )

    def __init__(self):
        super().__init__(FrameType.ROUTE_REMOVE)

    def parse(self, buffer: bytes, offset: int):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH

        self.broker_id = buffer[offset:offset + 16]
        offset += 16
        self.route_id = buffer[offset:offset + 16]
        offset += 16
        self.timestamp = struct.unpack('>Q', buffer[offset:offset + 8])[0]

    def serialize(self, middle=b'', flags=0) -> bytes:
        middle += self.broker_id
        middle += self.route_id
        middle += struct.pack('>Q', self.timestamp)
        return Frame.serialize(self, middle)


class BrokerInfoFrame(Frame):
    __slots__ = (
        'broker_id',
        'timestamp',
        'key_value_map'
    )

    def __init__(self):
        super().__init__(FrameType.ADDRESS)
        self.broker_id = None
        self.timestamp = None
        self.key_value_map = {}

    def parse(self, buffer: bytes, offset: int):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH

        self.broker_id = buffer[offset:offset + 16]
        offset += 16
        self.timestamp = struct.unpack('>Q', buffer[offset:offset + 8])[0]
        offset += 8
        self.key_value_map, _ = parse_key_value_map(buffer, offset)

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        middle += self.broker_id

        middle += struct.pack('>Q', self.timestamp)

        middle += serialize_key_value(self.key_value_map)

        return Frame.serialize(self, middle, flags)


class AddressFrame(Frame):
    __slots__ = (
        'origin_route_id',
        'key_value_map',
        'flag_encrypted',
        'flag_unicast',
        'flag_multicast',
        'flag_shared_routing',
        'metadata',
    )

    def __init__(self):
        super().__init__(FrameType.ADDRESS)
        self.origin_route_id = None
        self.flag_encrypted = False
        self.flag_unicast = False
        self.flag_multicast = False
        self.flag_shared_routing = False
        self.key_value_map = {}
        self.metadata = b''

    def parse(self, buffer, offset: int):
        flags = parse_header(self, buffer, offset)
        offset += HEADER_LENGTH

        self.flag_encrypted = is_flag_set(flags, _FLAG_ENCRYPTED_BIT)
        self.flag_unicast = is_flag_set(flags, _FLAG_UNICAST_BIT)
        self.flag_multicast = is_flag_set(flags, _FLAG_MULTICAST_BIT)
        self.flag_shared_routing = is_flag_set(flags, _FLAG_SHARED_ROUTING_BIT)

        self.origin_route_id = buffer[offset:offset + 16]
        offset += 16
        self.key_value_map, offset = parse_key_value_map(buffer, offset)
        self.metadata = buffer[offset:]

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        flags &= ~_FLAG_ENCRYPTED_BIT

        if self.flag_unicast:
            flags |= _FLAG_UNICAST_BIT

        if self.flag_multicast:
            flags |= _FLAG_MULTICAST_BIT

        if self.flag_shared_routing:
            flags |= _FLAG_SHARED_ROUTING_BIT

        middle += self.origin_route_id

        middle += serialize_key_value(self.key_value_map)
        middle += self.metadata

        return Frame.serialize(self, middle, flags)


_frame_class_by_id = {
    FrameType.ROUTE_SETUP: RouteSetupFrame,
    FrameType.ROUTE_ADD: RouteAddFrame,
    FrameType.ROUTE_REMOVE: RouteRemoveFrame,
    FrameType.BROKER_INFO: BrokerInfoFrame,
    FrameType.ADDRESS: AddressFrame,
}


def parse_or_ignore(buffer: bytes) -> Optional[Frame]:
    if len(buffer) < HEADER_LENGTH:
        raise ParseError('Frame too short: {} bytes'.format(len(buffer)))

    header = Header()
    parse_header(header, buffer, 0)

    try:
        frame = _frame_class_by_id[header.frame_type]()
    except KeyError as exception:
        raise RSocketUnknownFrameType(header.frame_type) from exception

    try:
        frame.parse(buffer, 0)
        return frame
    except Exception as exception:
        if not header.flags_ignore:
            raise RSocketProtocolError(ErrorCode.CONNECTION_ERROR, str(exception)) from exception

