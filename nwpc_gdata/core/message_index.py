import typing

import attr
import eccodes


@attr.s
class GribMessageIndex(object):
    offset = attr.ib()
    length = attr.ib()
    file_path = attr.ib()


def load_bytes_from_index(index: GribMessageIndex) -> typing.Optional[bytes]:
    with open(index.file_path, "rb") as f:
        f.seek(index.offset)
        message = eccodes.codes_grib_new_from_file(f)
        if message is None:
            return None
        raw_bytes = eccodes.codes_get_message(message)
        return raw_bytes
