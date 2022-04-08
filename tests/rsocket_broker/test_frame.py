from rsocket_broker.frame import RouteSetupFrame, FrameType, BrokerInfoFrame, RouteAddFrame, RouteRemoveFrame
from tests.rsocket_broker.helpers import data_bits, build_frame, bits


def test_route_setup_frame():
    items = [
        bits(16, 0, 'Major version'),
        bits(16, 1, 'Minor version'),
        bits(6, 1, 'Frame type'),
        bits(10, 0, 'Padding flags'),
        data_bits(b'1234567890123456', 'RouteID'),
        bits(8, 5, 'Service name length'),
        data_bits(b'12345'),
        bits(1, 0, 'Not well known tag'),
        bits(7, 7, 'tag length'),
        data_bits(b'abcdefgh'),
        bits(1, 0, 'Has next value'),
        bits(7, 7, 'value length'),
        data_bits(b'01234567'),
    ]

    frame_data = build_frame(*items)
    frame = RouteSetupFrame()
    frame.parse(frame_data, 0)

    assert frame.major_version == 0
    assert frame.minor_version == 1
    assert frame.route_id == b'1234567890123456'
    assert frame.service_name == b'12345'
    assert frame.key_value_map[b'abcdefgh'] == b'01234567'
    assert frame.frame_type is FrameType.ROUTE_SETUP

    assert frame.serialize() == frame_data


def test_broker_info_frame():
    items = [
        bits(16, 0, 'Major version'),
        bits(16, 1, 'Minor version'),
        bits(6, 4, 'Frame type'),
        bits(10, 0, 'Padding flags'),
        data_bits(b'1234567890123456', 'BrokerID'),
        bits(64, 123, 'Timestamp'),
        bits(1, 0, 'Not well known tag'),
        bits(7, 7, 'tag length'),
        data_bits(b'abcdefgh'),
        bits(1, 0, 'Has next value'),
        bits(7, 7, 'value length'),
        data_bits(b'01234567'),
    ]

    frame_data = build_frame(*items)
    frame = BrokerInfoFrame()
    frame.parse(frame_data, 0)

    assert frame.major_version == 0
    assert frame.minor_version == 1
    assert frame.broker_id == b'1234567890123456'
    assert frame.timestamp == 123
    assert frame.key_value_map[b'abcdefgh'] == b'01234567'
    assert frame.frame_type is FrameType.BROKER_INFO

    assert frame.serialize() == frame_data


def test_route_add_frame():
    items = [
        bits(16, 0, 'Major version'),
        bits(16, 1, 'Minor version'),
        bits(6, 2, 'Frame type'),
        bits(10, 0, 'Padding flags'),
        data_bits(b'1234567890123456', 'BrokerID'),
        data_bits(b'1234567890123456', 'RouteId'),
        bits(64, 123, 'Timestamp'),
        bits(8, 5, 'Service name length'),
        data_bits(b'12345'),
        bits(1, 0, 'Not well known tag'),
        bits(7, 7, 'tag length'),
        data_bits(b'abcdefgh'),
        bits(1, 0, 'Has next value'),
        bits(7, 7, 'value length'),
        data_bits(b'01234567'),
    ]

    frame_data = build_frame(*items)
    frame = RouteAddFrame()
    frame.parse(frame_data, 0)

    assert frame.major_version == 0
    assert frame.minor_version == 1
    assert frame.broker_id == b'1234567890123456'
    assert frame.route_id == b'1234567890123456'
    assert frame.timestamp == 123
    assert frame.service_name == b'12345'
    assert frame.key_value_map[b'abcdefgh'] == b'01234567'
    assert frame.frame_type is FrameType.ROUTE_ADD

    assert frame.serialize() == frame_data


def test_route_remove_frame():
    items = [
        bits(16, 0, 'Major version'),
        bits(16, 1, 'Minor version'),
        bits(6, 3, 'Frame type'),
        bits(10, 0, 'Padding flags'),
        data_bits(b'1234567890123456', 'BrokerID'),
        data_bits(b'1234567890123456', 'RouteId'),
        bits(64, 123, 'Timestamp')
    ]

    frame_data = build_frame(*items)
    frame = RouteRemoveFrame()
    frame.parse(frame_data, 0)

    assert frame.major_version == 0
    assert frame.minor_version == 1
    assert frame.broker_id == b'1234567890123456'
    assert frame.route_id == b'1234567890123456'
    assert frame.timestamp == 123
    assert frame.frame_type is FrameType.ROUTE_REMOVE

    assert frame.serialize() == frame_data
