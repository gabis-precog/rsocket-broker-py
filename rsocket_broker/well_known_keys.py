from enum import Enum, unique
from typing import Optional

from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import WellKnownType

from rsocket_broker.exceptions import RSocketBrokerUnknownKey


class WellKnownKey(WellKnownType):
    pass


@unique
class WellKnownKeys(Enum):

    TAG_NoTagPresent = WellKnownKey(b'No Tag Present', 0x00)
    TAG_ServiceName = WellKnownKey(b'io.rsocket.routing.ServiceName', 0x01)
    TAG_RouteId = WellKnownKey(b'io.rsocket.routing.RouteId', 0x02)
    TAG_InstanceName = WellKnownKey(b'io.rsocket.routing.InstanceName', 0x03)
    TAG_ClusterName = WellKnownKey(b'io.rsocket.routing.ClusterName', 0x04)
    TAG_Provider = WellKnownKey(b'io.rsocket.routing.Provider', 0x05)
    TAG_Region = WellKnownKey(b'io.rsocket.routing.Region', 0x06)
    TAG_Zone = WellKnownKey(b'io.rsocket.routing.Zone', 0x07)
    TAG_Device = WellKnownKey(b'io.rsocket.routing.Device', 0x08)
    TAG_OS = WellKnownKey(b'io.rsocket.routing.OS', 0x09)
    TAG_UserName = WellKnownKey(b'io.rsocket.routing.UserName', 0x0A)
    TAG_UserId = WellKnownKey(b'io.rsocket.routing.UserId', 0x0B)
    TAG_MajorVersion = WellKnownKey(b'io.rsocket.routing.MajorVersion', 0x0C)
    TAG_MinorVersion = WellKnownKey(b'io.rsocket.routing.MinorVersion', 0x0D)
    TAG_PatchVersion = WellKnownKey(b'io.rsocket.routing.PatchVersion', 0x0E)
    TAG_Version = WellKnownKey(b'io.rsocket.routing.Version', 0x0F)
    TAG_Environment = WellKnownKey(b'io.rsocket.routing.Environment', 0x10)
    TAG_TestCell = WellKnownKey(b'io.rsocket.routing.TestCell', 0x11)
    TAG_DNS = WellKnownKey(b'io.rsocket.routing.DNS', 0x12)
    TAG_IPv4 = WellKnownKey(b'io.rsocket.routing.IPv4', 0x13)
    TAG_IPv6 = WellKnownKey(b'io.rsocket.routing.IPv6', 0x14)
    TAG_Country = WellKnownKey(b'io.rsocket.routing.Country', 0x15)
    TAG_TimeZone = WellKnownKey(b'io.rsocket.routing.TimeZone', 0x1A)
    TAG_ShardKey = WellKnownKey(b'io.rsocket.routing.ShardKey', 0x1B)
    TAG_ShardMethod = WellKnownKey(b'io.rsocket.routing.ShardMethod', 0x1C)
    TAG_StickyRouteKey = WellKnownKey(b'io.rsocket.routing.StickyRouteKey', 0x1D)
    TAG_LBMethod = WellKnownKey(b'io.rsocket.routing.LBMethod', 0x1E)

    @classmethod
    def require_by_id(cls, key_numeric_id: int) -> WellKnownKey:
        for value in cls:
            if value.value.id == key_numeric_id:
                return value.value

        raise RSocketBrokerUnknownKey(key_numeric_id)

    @classmethod
    def get_by_name(cls, key_name: str) -> Optional[WellKnownKey]:
        for value in cls:
            if value.value.name == key_name:
                return value.value

        return None


def ensure_encoding_name(encoding) -> bytes:
    if isinstance(encoding, WellKnownKeys):
        return encoding.value.name

    return ensure_bytes(encoding)
