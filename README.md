# Benchmarking (Multiscale) Shapes for `SpatialData`

Our investigation at the Paris 2025 scverse hackathon will be around shapes, with a focus on multiscale implementations for `SpatialData`.  For multiscale data, we will be using the [`TileJSON` specification](https://polusai.github.io/microjson/tiling/), which is agnostic to the on-disk format.  For a brief overview of this task, please see the github issue: https://github.com/scverse/202503_hackathon_owkin/issues/4

## Getting started

In the notebooks folder is a `geopandas.ipynb` notebook that walks through the format of shapes in geopandas and parquet (i.e., https://geoarrow.org/format.html) and how we can link it with `TileJSON`. 

To set up the environment here, you can simply do (using [`uv`](https://github.com/astral-sh/uv))

```
uv venv venv
source venv/bin/activate
uv pip install -e ".[dev]"
```

These commands give you an editable version of what you have locally written in live environments as well as the dependencies I thought would be helpful at the time of writing.  If you do for the `microjson` repo, you can do something like

```
uv pip install -e path/to/microjson
```

to be able to develop with it locally in the environment you use here (may include benchmarking, hacking with notebooks etc).


## Task details

Here I'll go into more detail about the tasks and what is needed:

- **zarr implementation of the spec**

Because the spec is agnostic, implementing it for zarr should not be an issue. To do this, you will likely need to [fork the repo for microjson](https://github.com/polusai/microjson) and implement it there under `TileWriter`.  We can then benchmark here against other implementations.

- **parquet implementation of the spec**
      
Similarly, implementing it for parquet should not be an issue.  Same thing applies - fork the repo, benchmark here.

- **benchmarking of the format against all implementations in python**
      
We won't just be comparing zarr vs parquet, but also the "normal" protobufs/microjson implementations already in the repo
      
- **Benchmark level-0 loading of tiles vs. other full-resolution methods of loading data**
      
Because the level-0 zoom level of the multiscale representations mentioned above are comparable in what they represent to the current possible implementations in geoparquet (which `SpatialData` does), we should see what the speed tradeoffs are between the above implementations and the current implementation for non-visualization tasks that will use the level-0 tiles
      
- **Investigate/Implement API access within spatialdata for both MicroJSON and TileJSON i.e., necessary data structures, new namespaces etc.**

More of a stretch goal, but a first step in looking at interoperability would be loading from the level-0 representation of `TileJSON` into `SpatialData`
      
- **Investigation of visualization in Napari**
      
Unrelated to the above, using the current `TileJSON` implemenation in Napari, at least at the level-0 zoom level should be possible.  A stretch goal here would be actually utilizaing the multiscale representation
      
- **Investigation of deck.gl-based visualization/layer**
      
The folks at PolusAI say they have a visualization layer that just "works" with the on-disk format.  If we don't have access to that, someone can write both a reader for the format (or one of the variants we create here), and then a visualization layer using something like deck.gl
      
