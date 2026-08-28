from pyspark import pipelines as dp

@dp.table(name="itens_prova")
def itens_prova():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .load("dbfs:/Volumes/databricks-repo/enem/enem2025/DADOS/ITENS_PROVA_2025.csv")
    )

@dp.table(name="participantes")
def participantes():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .load("dbfs:/Volumes/databricks-repo/enem/enem2025/DADOS/PARTICIPANTES_2025.csv")
    )
