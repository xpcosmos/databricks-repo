from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
import inspect

class Data:
  def __init__(self, source:str, target:str):
    self.source:str = source
    self.target:str = target

  def spark(self) -> SparkSession:
    return SparkSession.builder.getOrCreate()
  
  def cp(self) -> bool:
    try:
      # Execute SQL command using Spark
      split_str = self.target.split('/')
      catalog_name = split_str[-3]
      schema_name = split_str[-2]
      volume_name = split_str[-1]

      spark = self.spark()

      spark.sql(f"""
          CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{volume_name}`
          COMMENT 'This is a managed volume created via Python'
      """)
      return dbutils.fs.cp(self.source, self.target, recurse=True)
    except Exception as e:
        print(f"Error: {e}")
        return False