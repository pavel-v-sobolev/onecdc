from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("cdc-1c")
except PackageNotFoundError:
    __version__ = "dev"

from .metadata_reader import MetadataReader1C
from .data_reader import DataReader1C, DataObject1C
from .change_reader import ChangeReader1C
from .name_mapper import NameMapper1C
from .db_writer import DBWriter1C
from .handlers import Handler1C, HandlerContext, HandlerLoop
from .replicator import Replicator1C
from .cron_runner import FullLoadCron

__all__ = ["MetadataReader1C", "DataReader1C", "DataObject1C", "ChangeReader1C", "NameMapper1C",
           "DBWriter1C", "Handler1C", "HandlerContext", "HandlerLoop", "Replicator1C",
           "FullLoadCron", "__version__"]

