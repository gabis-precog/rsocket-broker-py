from typing import Dict

from rsocket.frame_helpers import parse_type, serialize_128max_value
from rsocket.helpers import serialize_well_known_encoding

from rsocket_broker.well_known_keys import WellKnownKeys


def parse_key_value_map(buffer: bytes, offset: int) -> Dict[bytes, bytes]:
    key_value_map = {}
    while offset < len(buffer):
        is_known_type, tag_length = parse_type(buffer[offset:])
        offset += 1
        tag_length = tag_length + 1
        if not is_known_type:
            tag = buffer[offset:offset + tag_length]
            offset += tag_length
        else:
            tag = WellKnownKeys.require_by_id(tag_length).name

        has_next_value, value_length = parse_type(buffer[offset:])
        offset += 1
        value_length = value_length + 1
        if value_length > 0:
            value = buffer[offset:offset + value_length]
            offset += value_length
        else:
            value = None

        key_value_map[tag] = value

        if not has_next_value:
            break

    return key_value_map


def serialize_key_value(key_value_map) -> bytes:
    key_value_count = len(key_value_map)
    middle = b''
    for index, (key, value) in enumerate(key_value_map.items()):
        middle += serialize_well_known_encoding(key, WellKnownKeys.get_by_name)

        if value is not None:
            value_bytes = serialize_128max_value(value)
        else:
            value_bytes = bytearray(1)

        has_next_tag = index != key_value_count - 1

        if has_next_tag:
            value_bytes[0] = value_bytes[0] & (0xff >> 1)

        middle += value_bytes
    return middle
