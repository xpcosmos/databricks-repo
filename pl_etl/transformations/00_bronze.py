from pyspark import pipelines as dp


CATALOG_NAME = 'databricks-repo'
SCHEMA_NAME = 'bronze'
VOLUME_NAME = 'raw_enem'

data_base_path = f"dbfs:/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/DADOS"

@dp.table(name="itens_prova")
def tb_itens_prova():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "iso-8859-1") 
        .load(data_base_path + "/ITENS_PROVA_2025.csv")
    )

@dp.table(name="participantes")
def tb_participantes():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "iso-8859-1")
        .load(data_base_path + "/PARTICIPANTES_2025.csv")
    )

@dp.table(name="resultados")
def tb_resultados():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "iso-8859-1")
        .load( data_base_path + "/RESULTADOS_2024.csv")
    )