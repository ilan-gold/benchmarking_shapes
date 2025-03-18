from enum import Enum
from types import EllipsisType
from typing import TypeAlias

Index1D: TypeAlias = slice | EllipsisType
Index: TypeAlias = Index1D | tuple[Index1D, Index1D] | None


class GeometryType(Enum):
    Shapes = "shapes"
    Points = "points"
