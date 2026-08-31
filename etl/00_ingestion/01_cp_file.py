import sys, os
from dbruntime.remotefshandler.RemoteFsHandler import *
sys.path.append(os.path.abspath('./src'))

from src import *

SOURCE_PATH = (
    "file:/Workspace/Users/"
    "mikeias.oliveira@al.infnet.edu.br/"
    "databricks-repo/etl/tmp/microdados_enem_2024"
)

TARGET_PATH = "dbfs:/Volumes/databricks-repo/bronze/raw_enem"

datafile = Data(SOURCE_PATH, TARGET_PATH)
datafile.cp()