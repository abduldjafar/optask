from abc import ABC, abstractmethod
import polars as pl
import os
from utils.audit import get_next_file

class BaseReader(ABC):
    """
    Abstract Base Class for Data Readers.
    All reader implementations must implement the read_next() method.
    """
    @abstractmethod
    def read_next(self, table_name: str) -> tuple[pl.DataFrame | None, str | None, str | None]:
        """
        Reads the next available data source.
        Returns:
            Tuple of (DataFrame or None, Filename or Identifier, Full Source Path or None)
        """
        pass

class LocalFileReader(BaseReader):
    """
    Base class for reading local files based on audit log state.
    """
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def get_next_source_path(self, table_name: str) -> tuple[str | None, str | None]:
        table_path = os.path.join(self.base_dir, table_name)
        next_file = get_next_file(table_name, table_path)
        if not next_file:
            return None, None
        return next_file, os.path.join(table_path, next_file)

class LocalCSVReader(LocalFileReader):
    def read_next(self, table_name: str) -> tuple[pl.DataFrame | None, str | None, str | None]:
        next_file, source_path = self.get_next_source_path(table_name)
        if not next_file:
            return None, None, None
            
        df = pl.read_csv(source_path)
        return df, next_file, source_path

class LocalJSONReader(LocalFileReader):
    def read_next(self, table_name: str) -> tuple[pl.DataFrame | None, str | None, str | None]:
        next_file, source_path = self.get_next_source_path(table_name)
        if not next_file:
            return None, None, None
            
        df = pl.read_json(source_path)
        return df, next_file, source_path
