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
    data_path = "../data"

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

# Filter on the cities with population between 100,000 and 300,000
g.vertices.filter("population > 100000 and population < 300000").sort(
    "population"
).show()

# Find the path from Den Haag to a medium-sized city
from_expr = "id='Den Haag'"
to_expr = "population > 100000 and population < 300000 and id <> 'Den Haag'"
result = g.bfs(from_expr, to_expr)

columns = [column for column in result.columns if not column.startswith("e")]
result.select(columns).show(truncate=False)
