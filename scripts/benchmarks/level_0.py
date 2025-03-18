#!/usr/bin/env python3

import asyncio
import json
import platform
import subprocess
import time
import timeit
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from enum import Enum
from pathlib import Path

import arguably
import geopandas
import shapely
import zarr
import zarrs  # noqa: F401
from zarr.api.asynchronous import open_group as open_group_async

from benchmarking_shapes.level_0 import (
    GeometryType,
    Index,
    TileReaderParquet,
    TileReaderZarr,
)

zarr.config.set({"codec_pipeline.path": "zarrs.ZarrsCodecPipeline"})


def clear_cache():
    if platform.system() == "Darwin":
        subprocess.call(["sync", "&&", "sudo", "purge"])
    elif platform.system() == "Linux":
        subprocess.call(["sudo", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"])
    else:
        msg = "Unsupported platform"
        raise RuntimeError(msg)


class Encoding(Enum):
    GEOARROW = "geoarrow"
    WKB = "WKB"


class FileFormat(Enum):
    GEOPARQUET = "geoparquet"
    ZARR = "zarr"


def parse_celldega(celldega_path: Path):
    first_dzi_file = next((celldega_path / "pyramid_images").glob("*.dzi"))
    root = ET.parse(first_dzi_file).getroot()
    size_element = root.find(".//{http://schemas.microsoft.com/deepzoom/2008}Size")
    if size_element is None:
        msg = "Could not find Size node"
        raise ValueError(msg)
    height = size_element.attrib["Height"]
    width = size_element.attrib["Width"]
    shape = (int(height), int(width))  # type: ignore
    with (celldega_path / "landscape_parameters.json").open() as f:
        landscape_parameters = json.loads(f.read())
    tile_size = landscape_parameters["tile_size"]
    return tile_size, shape


def open_parquet(
    celldega_path: Path,
    geometry_type: GeometryType,
    encoding: Encoding,
) -> TileReaderParquet:
    tile_size, shape = parse_celldega(celldega_path)
    return TileReaderParquet(
        shape=shape,
        tile_size=tile_size,
        encoding=encoding.value.lower(),
        path_str_template=Path("../conversion/data/tiled").absolute()
        / encoding.value
        / ("shapes" if geometry_type == GeometryType.Shapes else "points")
        / celldega_path.name
        / "geo_parquet_{x}_{y}.parquet",
    )


async def open_zarr(
    celldega_path: Path,
):
    tile_size, shape = parse_celldega(celldega_path)
    parent_group = await open_group_async(
        Path(f"../conversion/data/tiled/zarr/shapes/{celldega_path.name}").absolute(),
        mode="r",
        use_consolidated=True,
    )
    return TileReaderZarr(parent_group=parent_group, tile_size=tile_size, shape=shape)


async def do_zarr(celldega_path: Path, index):
    reader = await open_zarr(celldega_path)
    await reader.getitem(index)


async def read_full_zarr(
    celldega_path: Path,
):
    name = celldega_path.name
    path = f"../conversion/data/single_file/zarr/shapes/{name}/geo.zarr"
    parent_group = await zarr.api.asynchronous.open_group(path, mode="r")
    buffer_arr, offsets_arr = await asyncio.gather(
        parent_group.get("buffer"), parent_group.get("offsets")
    )
    buffer, offsets = await asyncio.gather(
        buffer_arr.getitem(()), offsets_arr.getitem(())
    )
    return geopandas.GeoDataFrame(
        {
            "geometry": shapely.from_ragged_array(
                shapely.GeometryType.POLYGON, buffer, offsets
            )
        }
    )


def index_generator(shape) -> Iterable[Index]:
    yield (slice(shape[0] // 2), Ellipsis)  # big part, half
    yield (
        slice((shape[0] // 3), (shape[0] // 5) * 2),
        slice((shape[1] // 3), (shape[1] // 5) * 2),
    )  # smaller section
    yield (slice(None), slice(None))  # full dataset


def index_parquet(reader: TileReaderParquet, index: Index):
    reader[index]


@arguably.command
async def benchmark(celldega_path: Path):
    geometry_type = GeometryType.Shapes
    _, shape = parse_celldega(celldega_path)
    for encoding in Encoding:
        parquet_tile_reader = open_parquet(celldega_path, geometry_type, encoding)
        for index in index_generator(shape):
            clear_cache()
            time_taken = timeit.timeit(
                lambda: index_parquet(parquet_tile_reader, index), number=5
            )
            print(
                f"Indexing a tiled geoparquet using encoding {encoding} with index {index} took {time_taken} seconds"
            )
    for index in index_generator(shape):
        clear_cache()
        t = time.time()
        time_taken = await do_zarr(celldega_path, index)
        time_taken = time.time() - t
        print(
            f"Indexing a tiled zarr using with index {index} took {time_taken} seconds"
        )
    for encoding in Encoding:
        clear_cache()
        time_taken = timeit.timeit(
            lambda: geopandas.read_parquet(
                f"../conversion/data/single_file/{encoding.value}/{('shapes' if geometry_type == GeometryType.Shapes else 'points')}/{celldega_path.name}/geo_parquet.parquet"
            ),
            number=5,
        )
        print(
            f"Reading a single geoparquet with encoding {encoding} took {time_taken} seconds"
        )
    t = time.time()
    await read_full_zarr(celldega_path)
    print("Reading a single zarr store took", time.time() - t)


if __name__ == "__main__":
    arguably.run()
