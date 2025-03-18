import os
import re
from enum import Enum
from pathlib import Path

import arguably
import geopandas
import numpy as np
import pyarrow.parquet
import shapely
import zarr
from shapely import Polygon

from benchmarking_shapes.level_0 import GeometryType


class Encoding(Enum):
    WKB = "WKB"
    GEOARROW = "geoarrow"


class FileFormat(Enum):
    GEOPARQUET = "geoparquet"
    ZARR = "zarr"


def table_to_geopandas(table: pyarrow.Table) -> geopandas.GeoDataFrame:
    return geopandas.GeoDataFrame(
        {
            "geometry": geopandas.GeoSeries(
                [Polygon(g.as_py()[0]) for g in table["GEOMETRY"]]
            )
        }  # ?
    )


@arguably.command
def to(
    celldega_path: Path,
    geometry_type: GeometryType,
    encoding: Encoding,
    file_format: FileFormat,
    *,
    should_spatial_index: bool = False,
    should_tile: bool = False,
):
    tile_directory = celldega_path / (
        "cell_segmentation"
        if geometry_type == GeometryType.Shapes
        else "transcript_tiles"
    )
    tile_files = list(tile_directory.glob("*.parquet"))
    if should_tile:
        for file in tile_files:
            table = pyarrow.parquet.ParquetDataset(file).read_pandas()
            gdf = table_to_geopandas(table)
            match = re.search(r"_(\d+)_([\d]+)", file.name)
            if match is None:
                msg = f"Incorrect file name {file.name}"
                raise ValueError(msg)
            if file_format == FileFormat.GEOPARQUET:
                file_name = f"data/tiled/{encoding.value}/{geometry_type.value}/{celldega_path.name}/geo_parquet/{match.group(1)}_{match.group(2)}.parquet"
                os.makedirs(os.path.dirname(file_name), exist_ok=True)  # noqa: PTH103, PTH120
                gdf.to_parquet(
                    file_name,
                    geometry_encoding=encoding.value,
                )
            else:
                ragged = shapely.to_ragged_array(gdf["geometry"])
                parent = zarr.open(
                    f"data/tiled/zarr/{geometry_type.value}/{celldega_path.name}"
                )
                z = parent.require_group(f"{match.group(1)}_{match.group(2)}.zarr")
                z["buffer"] = ragged[1]
                z["offsets"] = np.vstack(ragged[2])
    else:
        table = pyarrow.parquet.ParquetDataset(tile_files).read_pandas()
        gdf = table_to_geopandas(table)
        if should_spatial_index:
            gdf.sindex  # generate spatial index
        if file_format == FileFormat.GEOPARQUET:
            file_name = f"data/single_file/{encoding.value}/{geometry_type.value}/{celldega_path.name}/{'si' if should_spatial_index else ''}/geo_parquet.parquet"
            os.makedirs(os.path.dirname(file_name), exist_ok=True)  # noqa: PTH103, PTH120
            gdf.to_parquet(
                file_name,
                geometry_encoding=encoding.value,
            )
        else:
            file_name = f"data/single_file/zarr/{geometry_type.value}/{celldega_path.name}/{'si' if should_spatial_index else ''}/geo.zarr"
            ragged = shapely.to_ragged_array(gdf["geometry"])
            z = zarr.open(file_name)
            z["buffer"] = ragged[1]
            z["offsets"] = np.vstack(ragged[2])


if __name__ == "__main__":
    arguably.run()
