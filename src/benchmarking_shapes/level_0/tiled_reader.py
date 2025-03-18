import abc
import asyncio
import math
import re
from collections.abc import AsyncGenerator, Iterable
from dataclasses import dataclass
from functools import reduce
from itertools import product
from pathlib import Path
from types import EllipsisType
from typing import Literal

import geoarrow.pyarrow as ga
import geopandas
import numpy as np
import pandas as pd
import pyarrow.parquet
import shapely
from zarr import AsyncArray, AsyncGroup

from ._types import Index, Index1D


@dataclass
class TileReader(abc.ABC):
    shape: tuple[int, int]
    tile_size: int

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


@dataclass
class TileReaderParquet(TileReader):
    encoding: Literal["wkb", "geoarrow"]
    path_str_template: Path

    def __getitem__(self, index: Index) -> pd.DataFrame | geopandas.GeoDataFrame:
        parsed_index = self._parse_index(index)
        tile_ids = self._index_to_tile_ids(parsed_index)
        tile_paths = [
            str(self.path_str_template).format(x=tile[0], y=tile[1])
            for tile in tile_ids
            if Path(str(self.path_str_template).format(x=tile[0], y=tile[1])).exists()
        ]
        if self.encoding == "wkb":
            df = pyarrow.parquet.ParquetDataset(tile_paths).read()
            as_arrow = ga.as_geoarrow(df["geometry"])
            return as_arrow
        return pyarrow.parquet.ParquetDataset(tile_paths).read_pandas()


@dataclass
class TileReaderZarr(TileReader):
    parent_group: AsyncGroup

    async def groups(self, tiles: set[tuple[int, int]]) -> AsyncGenerator[AsyncGroup]:
        async for file, group in self.parent_group.members():
            match = re.search(r"(\d+)_([\d]+)", file)
            if match is None or isinstance(group, AsyncArray):
                raise ValueError(file)
            tile_id = (int(match.group(1)), int(match.group(2)))
            if tile_id in tiles:
                yield group

    async def _get_tile_ragged(
        self, group: AsyncGroup
    ) -> tuple[np.ndarray, np.ndarray]:
        buffer_arr, offset_arr = await asyncio.gather(
            group.get("buffer"), group.get("offsets")
        )
        if any(
            isinstance(arr, AsyncGroup) or arr is None
            for arr in [buffer_arr, offset_arr]
        ):
            raise ValueError(buffer_arr, offset_arr)
        return await asyncio.gather(buffer_arr.getitem(()), offset_arr.getitem(()))

    async def getitem(self, index: Index):
        parsed_index = self._parse_index(index)
        tile_ids = set(self._index_to_tile_ids(parsed_index))
        awaitables = []
        async for group in self.groups(tile_ids):
            awaitables += [self._get_tile_ragged(group)]
        ragged_tiles = await asyncio.gather(*awaitables)
        buffer = np.vstack([tile[0] for tile in ragged_tiles])
        offsets_1 = reduce(
            lambda curr_offsets, tile: np.concatenate(
                [
                    curr_offsets,
                    tile[1][0][1:] + curr_offsets[-1],
                ]
            ),
            ragged_tiles[1:],
            ragged_tiles[0][1][0],
        )
        offsets_2 = np.arange(len(offsets_1))
        return geopandas.GeoDataFrame(
            {
                "geometry": shapely.from_ragged_array(
                    shapely.GeometryType.POLYGON, buffer, (offsets_1, offsets_2)
                )
            }
        )
