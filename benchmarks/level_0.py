#!/usr/bin/env python3

import json
import timeit
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from enum import Enum
from pathlib import Path

import arguably

from benchmarking_shapes.level_0 import CellDegaTileReader, Index


class GeometryType(Enum):
    Shapes = 1
    Points = 0


def open_celldega(
    celldega_path: Path, geometry_type: GeometryType
) -> CellDegaTileReader:
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
    return CellDegaTileReader(
        shape=shape,
        tile_size=tile_size,
        path_str_template=celldega_path
        / (
            "cell_segmentation/cell_tile_{x}_{y}.parquet"
            if geometry_type == GeometryType.Shapes
            else "transcript_tiles/transcripts_tile_{x}_{y}.parqet"
        ),
    )


def index_generator(shape) -> Iterable[Index]:
    yield (slice(shape[0] // 2), Ellipsis)  # big part, half
    yield (
        slice((shape[0] // 3), (shape[0] // 5) * 2),
        slice((shape[1] // 3), (shape[1] // 5) * 2),
    )  # smaller section
    yield (slice(None), slice(None))  # full dataset


def index_celldega(reader: CellDegaTileReader, index: Index):
    reader[index]


@arguably.command
def benchmark(celldega_path: Path, geometry_type: GeometryType):
    celldega_tile_reader = open_celldega(celldega_path, geometry_type)
    for index in index_generator(celldega_tile_reader.shape):
        time_taken = timeit.timeit(
            lambda: index_celldega(celldega_tile_reader, index), number=5
        )
        print(f"Indexing using {index} took {time_taken} seconds")


if __name__ == "__main__":
    arguably.run()
