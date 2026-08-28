from databricks.sdk.runtime import dbutils

class Data:
  def __init__(self, source:str, target:str):
    self.source:str = source
    self.target:str = target
  def cp(self) -> bool:
    return dbutils.fs.cp(self.source, self.target, recurse=True)