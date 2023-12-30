from pyspark.sql.types import *
from graphframes import *

from pyspark.sql import SparkSession

# Execution command : spark-submit --driver-memory 2g --executor-memory 6g --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 importing_data.py

spark = (
    SparkSession.builder.master("local[1]")
    .appName("importing-transport-data")
    .getOrCreate()
)


def create_transport_graph():
    data_path = "/home/ahmed/Documents/books/Graph-Algorithms/0636920233145/data"

    node_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("population", IntegerType(), True),
        ]
    )

    nodes = spark.read.csv(
        data_path + "/transport-nodes.csv",
        header=True,
        schema=node_schema,
    )

    rels = spark.read.csv(
        data_path + "/transport-relationships.csv",
        header=True,
    )
    # Reversing the relationships to create a two way directed graph since the data is undirected.
    reversed_rels = (
        rels.withColumn("newSrc", rels.dst)
        .withColumn("newDst", rels.src)
        .drop("dst", "src")
        .withColumnRenamed("newSrc", "src")
        .withColumnRenamed("newDst", "dst")
        .select("src", "dst", "relationship", "cost")
    )

    relationships = rels.union(reversed_rels)

    return GraphFrame(nodes, relationships)


g = create_transport_graph()
