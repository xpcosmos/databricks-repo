from src import *

SOURCE_PATH = (
    "file:/Workspace/Users/"
    "mikeias.oliveira@al.infnet.edu.br/"
    "databricks-repo/etl/microdados_enem_2025"
)

TARGET_PATH = "dbfs:/Volumes/databricks-repo/enem/enem2025"

datafile = Data(SOURCE_PATH, TARGET_PATH)
datafile.cp()