import math
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from types import EllipsisType

import pyarrow as pa
import pyarrow.parquet

from ._types import Index, Index1D


@dataclass
class CellDegaTileReader:
    shape: tuple[int, int]
    tile_size: int
    path_str_template: Path

    def _parse_index(self, index: Index) -> tuple[Index1D, Index1D]:
        if isinstance(index, tuple):
            match len(index):
                case 2:
                    return index
                case 1:
                    return index[0], slice(None)
                case 0:
                    return slice(None), slice(None)
                case _:
                    msg = "Cannot parse length >2 size tuple indices"
                    raise ValueError(msg)
        if isinstance(index, Index1D):
            return index, slice(None)
        if index is None:
            return slice(None), slice(None)
        if isinstance(index, Ellipsis):
            return slice(None), slice(None)
        msg = f"Cannot recognize index of type {type(index)}"
        raise TypeError(msg)

    def _axis_index_to_tile_id(self, axis_index: Index1D, axis: int) -> list[int]:
        if isinstance(axis_index, slice):
            start, stop = axis_index.start or 0, axis_index.stop or self.shape[axis]
            start_tile, stop_tile = (
                math.floor(start // self.tile_size),
                math.ceil(stop // self.tile_size),
            )
            return list(range(start_tile, stop_tile))
        if isinstance(axis_index, EllipsisType):
            return list(range(self.shape[axis] // self.tile_size))
        msg = f"Unrecognized axis index of type {type(axis_index)}"
        raise TypeError(msg)

    def _index_to_tile_ids(
        self, index: tuple[Index1D, Index1D]
    ) -> Iterable[tuple[int, int]]:
        x_tile_ids = self._axis_index_to_tile_id(index[0], 0)
        y_tile_ids = self._axis_index_to_tile_id(index[1], 1)
        return product(x_tile_ids, y_tile_ids)

    def __getitem__(self, index: Index) -> pa.Table:
        parsed_index = self._parse_index(index)
        tile_ids = self._index_to_tile_ids(parsed_index)
        tile_paths = [
            str(self.path_str_template).format(x=tile[0], y=tile[1])
            for tile in tile_ids
            if Path(str(self.path_str_template).format(x=tile[0], y=tile[1])).exists()
        ]
        return pyarrow.parquet.ParquetDataset(tile_paths).read()
