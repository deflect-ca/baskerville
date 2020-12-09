# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from collections import defaultdict

from pyspark import SparkConf
from pyspark.ml.classification import RandomForestClassifier
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark_iforest.ml.iforest import IForest
import tempfile

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
from shap.explainers._tree import IsoTree
from sklearn.ensemble import IsolationForest

from baskerville.util.model_interpretation.helpers import \
    get_spark_session_with_iforest, Tree

spark = get_spark_session_with_iforest()
temp_path = tempfile.mkdtemp()
iforest_path = temp_path + "/iforest"
model_path = temp_path + "/iforest_model"

# same data as in https://gist.github.com/mkaranasou/7aa1f3a28258330679dcab4277c42419
# for comparison
data = [
    {'feature1': 1., 'feature2': 0., 'feature3': 0.3, 'feature4': 0.01,
     'label': 0},
    {'feature1': 10., 'feature2': 3., 'feature3': 0.9, 'feature4': 0.1,
     'label': 1},
    {'feature1': 101., 'feature2': 13., 'feature3': 0.9, 'feature4': 0.91,
     'label': 1},
    {'feature1': 111., 'feature2': 11., 'feature3': 1.2, 'feature4': 1.91,
     'label': 1},
]

# use a VectorAssembler to gather the features as Vectors (dense)
assembler = VectorAssembler(
    inputCols=list(data[0].keys()),
    outputCol="features"
)

df = spark.createDataFrame(data)
df = assembler.transform(df)
df.show()

scaler = StandardScaler(inputCol='features', outputCol='scaledFeatures')
iforest = IForest(contamination=0.3, maxDepth=2)
dt = DecisionTree()
iforest.setSeed(42)  # for reproducibility

feature_names = [c for c in df.columns if 'feature' in c]
df_no_label = df.select(feature_names)
scaler_model = scaler.fit(df_no_label)
df_no_label = scaler_model.transform(df_no_label)
df_no_label = df_no_label.withColumn('features', F.col('scaledFeatures')).drop(
    'scaledFeatures')
model = iforest.fit(df_no_label)

dt_data = [
    LabeledPoint(0.0, [0.0]),
    LabeledPoint(0.0, [1.0]),
    LabeledPoint(1.0, [2.0]),
    LabeledPoint(1.0, [3.0])
]
# >> > model = RandomForest.trainClassifier(sc.parallelize(data), 2, {}, 3,
#                                           seed=42)
# spark.sparkContext.parallelize(data)
dt_model = DecisionTree.trainClassifier(
    spark.sparkContext.parallelize(dt_data), numClasses=2,
    categoricalFeaturesInfo={},
    impurity='gini', maxDepth=5, maxBins=32)

dt_model.toDebugString()
rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="label", seed=42)
rf_model = rf.fit(df)

# https://www.timlrx.com/2018/06/19/feature-selection-using-feature-importance-score-creating-a-pyspark-estimator/
print('featureImportances', rf_model.featureImportances)

imodel = iforest.fit(df)
imodel.write().overwrite().save('testimodel')

node = spark.sparkContext._jvm.org.apache.spark.ml.iforest.IFLeafNode
model = spark.sparkContext._jvm.org.apache.spark.ml.iforest.IForestModel
lmodel = model.load('testimodel')
model_df = spark.read.parquet('testimodel/data')
model_df.select(F.max('treeId')).show()
# +-----------+
# |max(treeId)|
# +-----------+
# |         99|
# +-----------+
print(model_df.select('nodeData').first())
model_df.show(10, False)
print(model_df.count())

model_df.select('nodeData.id', 'nodeData.featureIndex',
                'nodeData.featureValue', 'nodeData.numInstance').show(10,
                                                                      False)
collected_data = model_df.collect()

scikit_if = IsolationForest()

trees = defaultdict(dict)

features = {}
for row in collected_data:
    trees[row.treeID][row.nodeData.id] = row.nodeData.asDict()
    features[row.nodeData.featureIndex] = True

print(trees)
print(features)

min_key = min(trees)
max_key = max(trees)
print(min_key, max_key)

import networkx as nx

g = nx.DiGraph()

nodes = []
edges = []

for i in range(min_key, max_key + 1):
    tree_node = f'tree_{i}'
    g.add_node(tree_node, )
    nodes.append((tree_node,))
    for k, v in trees[i].items():
        n_node = f'{tree_node}_node_{k}'
        print(n_node, str(v))
        g.add_node(n_node, values=str(v))
        nodes.append((n_node, str(v)))
        g.add_edge(tree_node, n_node)
        edges.append((tree_node, n_node))
        if v['leftChild'] > -1:
            g.add_edge(n_node, f'{tree_node}_node_{v["leftChild"]}')
            edges.append((n_node, f'{tree_node}_node_{v["leftChild"]}'))
        if v['rightChild'] > -1:
            g.add_edge(n_node, f'{tree_node}_node_{v["rightChild"]}')
            edges.append((n_node, f'{tree_node}_node_{v["rightChild"]}'))



# plt.rcParams["figure.figsize"] = (20, 20)
# nx.draw(g, node_size=1200, \
#     node_color='lightblue', linewidths=0.25, font_size=10, \
#     font_weight='bold', with_labels=True)
# # pos=nx.nx_pydot.graphviz_layout(g),
# plt.show()
# print(g)

# from graph_tools import Graph
#
# # create a graph with four nodes and two edges
# g = Graph(directed=True)
# g.add_edge(1, 2)
# g.add_edge(2, 3)
# g.add_vertex(4)
# g.add_edge(2, 4)
# g.add_edge(4, 3)
#
# print(g)
#
# # find the all shortest paths from vertex 1
# dist, prev = g.dijkstra(1)
# print(dist)
#
# # generate BA graph with 100 vertices
# g = Graph(directed=False).create_graph('barabasi', 100)
#
# # check if all vertices are mutually connected
# print(g.is_connected())
#
# # compute the betweenness centrality of vertex 1
# print(g.betweenness(1))


# # Vertex DataFrame
# v = spark.createDataFrame([
#   ("a", "Alice", 34),
#   ("b", "Bob", 36),
#   ("c", "Charlie", 30),
#   ("d", "David", 29),
#   ("e", "Esther", 32),
#   ("f", "Fanny", 36),
#   ("g", "Gabby", 60)
# ], ["id", "name", "age"])
# # Edge DataFrame
# e = spark.createDataFrame([
#   ("a", "b", "friend"),
#   ("b", "c", "follow"),
#   ("c", "b", "follow"),
#   ("f", "c", "follow"),
#   ("e", "f", "follow"),
#   ("e", "d", "friend"),
#   ("d", "a", "friend"),
#   ("a", "e", "friend")
# ], ["src", "dst", "relationship"])
# # Create a GraphFrame
# g = GraphFrame(v, e)
# g.edges.show()


# for i in range(min_key, max_key + 1):

# Row(nodeData=Row(id=0, featureIndex=2, featureValue=2.1980993960346815, leftChild=1, rightChild=2, numInstance=0))
# do this afterwards:
# https://stackoverflow.com/questions/49805284/how-to-get-node-information-on-spark-decision-tree-model
# trees = imodel._call_java('trees')
# print(trees)
# imodel.getTrees()
# print(lmodel.trees)
# model_inst = model('dage3q2q3', [node(10)])

# png_string = plot_tree(loaded_model,
#                        featureNames=list(data[0].keys()),
#                        categoryNames={},
#                        classNames=['abnormal', 'normal'],
#                        filled=True,
#                        roundedCorners=True,
#                        roundLeaves=True)
#
# import base64
# with open("imageToSave.png", "wb") as fh:
#     fh.write(base64.decodebytes(png_string))


# https://stats.stackexchange.com/a/393437/140260
# I believe it was not implemented in scikit-learn because in contrast with Random Forest algorithm, Isolation Forest feature to split at each node is selected at random. So it is not possible to have a notion of feature importance similar to RF.
#
# Having said that, If you are very confident about the results of Isolation Forest
# classifier and you have a capacity to train another model then you could use the
# output of Isolation Forest i.e -1/1 values as target-class to train a Random Forest classifier.
# This will give you feature importance for detecting anomaly.

# Despite all this the iForest is interpretable if its underlying principle is
# taken advantage of: that outliers have short decision paths on average over all
# the trees in the iForest. The features that were cut on in these short paths must
# be more important than those cut on in long decision paths. So here's what
# you can do to get feature importances:


# https://github.com/scikit-learn/scikit-learn/issues/10642
# SHAP tree explainer
# https://towardsdatascience.com/explain-your-model-with-the-shap-values-bc36aac4de3d
# https://github.com/dataman-git/codes_for_articles/blob/master/Explain%20your%20model%20with%20the%20SHAP%20values%20for%20article.ipynb

# the way to calculate the Shapley value: It is the average of the marginal contributions across all permutations.

import shap
import pandas as pd
pd_df = df.toPandas()
iforest_sk = IsolationForest()
x_train = pd.DataFrame([dv.values for dv in pd_df['features'].values], columns=feature_names)
im = iforest_sk.fit(x_train)
shap_values = shap.TreeExplainer(im).shap_values(x_train)
shap.summary_plot(shap_values, x_train, plot_type="bar")


FEATURES = df_no_label.columns
TREES = {}

for tree, nodes in trees.items():
    t = Tree(tree, nodes)
    TREES[tree] = t

scaling = 1.0 / len(TREES)
pd_df = df.toPandas()
x_train = [dv.values for dv in pd_df['features'].values.tolist()]
data_missing = []
shap_trees = []

for tid, tree in TREES.items():
    itree = IsoTree(
        tree,
        tree.features,
        scaling=scaling,
        data=x_train,
        data_missing=data_missing
    )
    shap_trees.append(itree)

# elif safe_isinstance(model, ["sklearn.ensemble.IsolationForest",
#                              "sklearn.ensemble.iforest.IsolationForest"]):
# self.dtype = np.float32
# scaling = 1.0 / len(model.estimators_)  # output is average of trees
# self.trees = [
#     IsoTree(e.tree_, f, scaling=scaling, data=data, data_missing=data_missing)
#     for e, f in zip(model.estimators_, model.estimators_features_)]
# self.tree_output = "raw_value"

