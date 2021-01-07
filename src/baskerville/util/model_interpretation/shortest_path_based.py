# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from graphframes import GraphFrame
from pyspark.sql import functions as F

#  https://towardsdatascience.com/an-implementation-and-explanation-of-the-random-forest-in-python-77bf308a9b76
# https://towardsdatascience.com/interpreting-random-forest-and-other-black-box-models-like-xgboost-80f9cc4a3c38
# https://towardsdatascience.com/how-to-visualize-a-decision-tree-from-a-random-forest-in-python-using-scikit-learn-38ad2d75f21c
# https://stackoverflow.com/questions/31782288/how-to-extract-rules-from-decision-tree-spark-mllib
# https://stackoverflow.com/questions/37129602/spark-mlib-decision-trees-probability-of-labels-by-features
# https://stackoverflow.com/questions/40558567/how-to-view-random-forest-statistics-in-spark-scala


# call scala from python
# https://diogoalexandrefranco.github.io/scala-code-in-pyspark/
# https://aseigneurin.github.io/2016/09/01/spark-calling-scala-code-from-pyspark.html
# node = spark.sparkContext._jvm.org.apache.spark.ml.iforest.IFLeafNode
# model = spark.sparkContext._jvm.org.apache.spark.ml.iforest.IForestModel
# https://sourceforge.net/p/py4j/mailman/py4j-users/thread/CAP%3DZQSvuw9Hnk_DHUtHBU6UWUmJqJBSTB_5rZPdrsf%2BeZnG_zA%40mail.gmail.com/#msg33108805

# graph x and graph-frames:
# http://spark.apache.org/docs/latest/graphx-programming-guide.html#
# https://graphframes.github.io/graphframes/docs/_site/quick-start.html


from baskerville.util.helpers import get_default_data_path
from baskerville.util.model_interpretation.helpers import \
    get_spark_session_with_iforest, load_anomaly_model, \
    get_trees_and_features, construct_tree_graph, get_shortest_path_for_g, \
    get_avg_shortest_path_for_forest, draw_graph

if __name__ == '__main__':
    data_path = get_default_data_path()
    test_model_path = f'{data_path}/models/AnomalyModel__2020_11_12___16_06_TestModel'
    test_model_data_path = f'{test_model_path}/iforest/data'
    spark = get_spark_session_with_iforest()
    # load test model
    anomaly_model = load_anomaly_model(test_model_path)
    # get features
    feature_names = anomaly_model.features.keys()
    nodes_df = spark.read.parquet(test_model_data_path)
    max_tree_id = nodes_df.select(F.max('treeId').alias('id')).collect()[0].id
    print(max_tree_id)
    print(nodes_df.select('nodeData').first())
    nodes_df.show(10, False)
    print(nodes_df.count())
    # https://www.timlrx.com/2018/06/19/feature-selection-using-feature-importance-score-creating-a-pyspark-estimator/


    nodes_df.select('nodeData.id', 'nodeData.featureIndex',
                    'nodeData.featureValue', 'nodeData.numInstance').show(10,
                                                                          False)
    # Row(treeID=112, nodeData=Row(id=94, featureIndex=-1, featureValue=-1.0, leftChild=-1, rightChild=-1, numInstance=1))
    trees, features = get_trees_and_features(nodes_df)
    g, nodes, edges = construct_tree_graph(trees, feature_names)
    # draw_graph(g)
    # avg_shortest_path = get_avg_shortest_path_for_forest(g)
    # print(avg_shortest_path)
    # todo: takes for ever
    # shortest_paths = get_shortest_path_for_g(g)
    #
    # print(shortest_paths)

    # Vertex DataFrame
    v = spark.createDataFrame(
        nodes,
        ["node", "id", "feature_index", "feature_value", "left", 'right', 'num_instances', 'feature']
    ).cache()
    v = v.withColumn('feature', F.when(F.col('feature').isNull(), 'END').otherwise(F.col('feature')))
    # Edge DataFrame
    e = spark.createDataFrame(edges, ["src", "dst", "rule"])
    # Create a GraphFrame
    g_spark = GraphFrame(v, e)
    g_spark.vertices.show()
    g_spark.edges.show()
    # print(g_spark.pageRank(sourceId=-100, maxIter=100))
    g_spark.shortestPaths([-100, 50])  # -100 is root
#