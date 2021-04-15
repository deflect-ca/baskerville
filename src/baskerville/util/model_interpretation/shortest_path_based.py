# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from graphframes import GraphFrame
from pyspark.sql import functions as F


from baskerville.util.helpers import get_default_data_path
from baskerville.util.model_interpretation.helpers import \
    get_spark_session_with_iforest, load_anomaly_model, \
    get_trees_and_features, construct_tree_graph

if __name__ == '__main__':
    data_path = get_default_data_path()
    test_model_path = f'{data_path}/models/AnomalyModel__2020_11_12___16_06_TestModel'
    test_model_data_path = f'{test_model_path}/iforest/data'
    spark = get_spark_session_with_iforest()
    # load test model
    anomaly_model = load_anomaly_model(test_model_path)
    # get features
    feature_names = anomaly_model.features.keys()
    nodes_df = spark.read.parquet(test_model_data_path).persist()
    max_tree_id = nodes_df.select(F.max('treeId').alias('id')).collect()[0].id
    print(max_tree_id)
    print(nodes_df.select('nodeData').first())
    nodes_df.show(10, False)
    print(nodes_df.count())

    nodes_df.select(
        'nodeData.id',
        'nodeData.featureIndex',
        'nodeData.featureValue',
        'nodeData.numInstance'
    ).show(10, False)
    # example:
    # Row(treeID=112, nodeData=Row(id=94, featureIndex=-1, featureValue=-1.0,
    # leftChild=-1, rightChild=-1, numInstance=1))

    # reconstruct trees
    trees, features = get_trees_and_features(nodes_df)

    # using networkx is not performant unfortunately
    g, nodes, edges = construct_tree_graph(trees, feature_names)
    # draw_graph(g)
    # avg_shortest_path = get_avg_shortest_path_for_forest(g)
    # print(avg_shortest_path)
    # todo: takes for ever
    # shortest_paths = get_shortest_path_for_g(g)
    # print(shortest_paths)

    # Using Graph Frames:
    v = spark.createDataFrame(
        nodes,
        [
            "name", "id", "feature_index", "feature_value", "left", 'right',
            'num_instances', 'feature'
        ]
    ).cache()
    v = v.withColumn(
        'feature',
        F.when(F.col('feature').isNull(), 'END').otherwise(F.col('feature'))
    )
    # Edge DataFrame
    e = spark.createDataFrame(edges, ["src", "dst", "rule"])
    # Create a GraphFrame
    g_spark = GraphFrame(v, e)
    g_spark.vertices.show()
    g_spark.edges.show()
    # print(g_spark.pageRank(sourceId=-100, maxIter=100))
    shortest_paths = g_spark.shortestPaths([-100])  # -100 is root
    print(shortest_paths)
