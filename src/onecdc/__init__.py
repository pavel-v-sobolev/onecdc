from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("onecdc")
except PackageNotFoundError:
    __version__ = "dev"

from .metadata_reader import MetadataReader
from .data_reader import DataReader, DataObject
from .change_reader import ChangeReader
from .name_mapper import NameMapper
from .db_writer import DBWriter
from .handlers import Handler, HandlerContext, HandlerLoop
from .replicator import Replicator
from .cron_runner import FullLoadCron

__all__ = ["MetadataReader", "DataReader", "DataObject", "ChangeReader", "NameMapper",
           "DBWriter", "Handler", "HandlerContext", "HandlerLoop", "Replicator",
           "FullLoadCron", "__version__"]

