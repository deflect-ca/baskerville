# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import operator
from random import shuffle
from collections import defaultdict

from pyspark import SparkConf
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession, Window
from pyspark_iforest.ml.iforest import IForestModel
from pyspark.sql import functions as F
from baskerville.models.anomaly_model import AnomalyModel
from baskerville.util.helpers import get_default_data_path

RANDOM_SEED = 42
FEATURES = []


class Feature:
    idx: int
    name: str
    value: float

    def __init__(self, idx, value):
        self.idx = idx
        self.value = value
        self.set_name()

    def set_name(self):
        self.name = FEATURES[self.idx]


class Node:
    id: int = None
    feature: Feature = None
    condition: str = ''
    num_instances: int = -1
    left_node: 'Node' = None
    right_node: 'Node' = None
    is_left: bool = True
    is_top: bool = False

    def __init__(self, node_data: dict, nodes: dict, is_left=True):
        self.id = node_data['id']
        self.feature = Feature(node_data['featureIndex'], node_data['featureValue'])
        self.is_left = is_left
        self.is_top = self.id == 0
        self.num_instances = node_data['numInstance']
        self.set_condition()
        self.set_nodes(node_data, nodes)

    def __str__(self):
        str_ln = 'N/A' if not self.left_node else str(self.left_node)
        str_rn = 'N/A' if not self.right_node else str(self.right_node)
        direction = 'top' if self.is_top else 'left' if self.is_left else 'right'
        self_str = f'{direction} node [#{self.num_instances}]: {self.condition}\n'
        self_str += 3 * '/                                             \\\n'
        self_str += 'LN                                                   RN\n'
        self_str += 'V                                                   V\n'
        self_str += f'{str_ln}                                              {str_rn}\n '
        return self_str

    def set_nodes(self, node_data, nodes):
        rn = node_data.get('rightChild', 0)
        ln = node_data.get('leftChild', 0)

        if rn > 0:
            self.right_node = Node(nodes[rn], nodes, is_left=False)
        if ln > 0:
            self.left_node = Node(nodes[ln], nodes, is_left=True)

    def set_condition(self):
        cnd = '<='
        if not self.is_left:
            cnd = '>'
        self.condition = f'{self.feature.name} {cnd} {self.feature.value}'


def get_node_features(node, total_features=None):
    if not total_features:
        total_features = {}
    total_features[node.feature.idx] = True
    if node.left_node:
        total_features = get_node_features(node.left_node, total_features)
    if node.right_node:
        total_features = get_node_features(node.right_node, total_features)
    return total_features


class Tree:
    id: int
    top_node: Node
    nodes: dict
    features: list = None

    def __init__(self, id, nodes):
        self.id = id
        self.nodes = nodes
        self.top_node = Node(nodes.get(0), nodes)
        self.features = self.get_features()

    def __str__(self):
        self_str = f'Tree {self.id}'
        self_str += 20 * '-'
        self_str += str(self.top_node)
        return self_str

    def get_features(self):
        return get_node_features(self.top_node)


def get_spark_session_with_iforest():
    import os
    from psutil import virtual_memory

    mem = virtual_memory()
    conf = SparkConf()
    iforest_jar = os.path.join(
        get_default_data_path(), 'jars', 'spark-iforest-2.4.0.99.jar'
    )
    # half of total memory - todo: should be configurable, use yaml or so
    memory = f'{int(round((mem.total/ 2)/1024/1024/1024, 0))}G'
    print(memory)
    conf.set('spark.jars', iforest_jar)
    conf.set('spark.driver.memory', memory)
    conf.set('spark.sql.shuffle.partitions', os.cpu_count())
    conf.set('spark.jars.packages',
             'julioasotodv:spark-tree-plotting:0.2,'
             'graphframes:graphframes:0.6.0-spark2.3-s_2.11'
             )

    conf.set('spark.driver.memory', '6G')

    return SparkSession \
        .builder \
        .config(conf=conf) \
        .appName("IForest feature importance") \
        .getOrCreate()


def load_anomaly_model(full_path) -> AnomalyModel:
    return AnomalyModel().load(full_path, get_spark_session_with_iforest())


def load_dataframe(full_path, features):
    from baskerville.features import FEATURE_NAME_TO_CLASS
    spark = get_spark_session_with_iforest()
    df = spark.read.json(full_path).cache()
    schema = spark.read.json(df.limit(1).rdd.map(lambda row: row.features)).schema
    df = df.withColumn(
        'features',
        F.from_json('features', schema)
    )
    for feature in features:
        column = f'features.{feature}'
        feature_class = FEATURE_NAME_TO_CLASS[feature]
        df = df.withColumn(column, F.col(column).cast(
            feature_class.spark_type()).alias(feature))
    return df


def load_dataframe_from_db(start, stop):
    raise NotImplementedError('Needs database configuration to get data')


def select_row(df, row_id):
    """
    Mark row as selected
    """
    return df.withColumn(
        'is_selected',
        F.when(F.col('id') == row_id, F.lit(True)).otherwise(F.lit(False))
    )


def get_sample_labeled_data():
    return [
    {'feature1': 10., 'feature2': 3., 'feature3': 0.9, 'feature4': 0.1,
     'label': 1},
    {'feature1': 1.12, 'feature2': 0.22, 'feature3': 0.38, 'feature4': 0.001,
     'label': 0},
    {'feature1': 101., 'feature2': 13., 'feature3': 0.9, 'feature4': 0.91,
     'label': 1},
    {'feature1': 43., 'feature2': 11., 'feature3': 1.2, 'feature4': 1.91,
     'label': 1},
    {'feature1': 2., 'feature2': 11., 'feature3': 1.2, 'feature4': 0.13,
     'label': 0},
    {'feature1': 132., 'feature2': 8., 'feature3': 1.2, 'feature4': 1.91,
     'label': 1},
    {'feature1': 99., 'feature2': 11., 'feature3': 1.2, 'feature4': 1.17,
     'label': 1},
    {'feature1': 123., 'feature2': 11., 'feature3': 1.2, 'feature4': 1.98,
     'label': 1},
    {'feature1': 440., 'feature2': 39., 'feature3': 1.2, 'feature4': 1.11,
     'label': 1},
    {'feature1': 4., 'feature2': 11., 'feature3': 0.9, 'feature4': 0.33,
     'label': 0},
    {'feature1': 1.4, 'feature2': 0.3, 'feature3': 1.2, 'feature4': 1.3,
     'label': 1},
    {'feature1': 101., 'feature2': 11., 'feature3': 1.2, 'feature4': 2.,
     'label': 1},
    {'feature1': 210.3, 'feature2': 11., 'feature3': 1.2, 'feature4': 3.1,
     'label': 1},
    {'feature1': 2., 'feature2': 1.5, 'feature3': 1.0, 'feature4': 0.3,
     'label': 0},
]


def get_sample_labeled_point_data():
    return [
    LabeledPoint(0.0, [0.0]),
    LabeledPoint(0.0, [1.0]),
    LabeledPoint(1.0, [2.0]),
    LabeledPoint(1.0, [3.0])
]


def gather_features_with_assembler(df, features):
    if not isinstance(features, list):
        features = list(features)
    # use a VectorAssembler to gather the features as Vectors (dense)
    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features"
    )
    return assembler.transform(df)


def calculate_shapley_values_for_all_features(
        df, features,
        model: IForestModel,
        scaler_model: StandardScalerModel = None,
        features_col: str = 'features',
        anomaly_score_col: str = 'anomalyScore'
):
    """
    # https://runawayhorse001.github.io/LearningApacheSpark/mc.html
    for a simulation of size N can be summarized as follows:
    Construct a list of p random seeds (where p is the number of worker nodes).
    Parallelize the list so that one random seed is present on each worker.
    A flatmap operation is applied to the random seeds. Each random seed is
    used to seed a multivariate Normal random number generator. Given a random
    vector sampled from the distribution, inner products are evaluated against
    different financial “instruments”. These are summed and the final value is
    considered the result of the trial . N/p trials are run on each worker.
    Since this operation is a flatmap, rather than a map, the result is a list
    of ~N double precision values corresponding to the result of each trial.
    The 5% VaR value corresponds to the trial value at index N/20 in the list
    of trial results, were the list sorted. In the Cloudera code, this is
    implemented by using the takeOrdered() function to retrieve the smallest
    N/20 elements from the list, then taking the final value.
    # https://cloud.google.com/solutions/monte-carlo-methods-with-hadoop-spark
    # https://christophm.github.io/interpretable-ml-book/shapley.html#estimating-the-shapley-value
    #  First, select an instance of interest x, a feature j and the number of
    #  iterations M. For each iteration, a random instance z is
    #  selected from the data and a random order of the features is generated.
    #  Two new instances are created by combining values from #  the instance
    #  of interest x and the sample z. The instance  x+j is the instance of
    #  interest, but all values in the order before  feature j are replaced by
    #  feature values from the sample z. The instance x−j is the same as x+j,
    #  but in addition has feature j #  replaced by the value for feature j
    #  from the sample z. The difference in the prediction from the black
    #  box is computed
    """
    model_features_col = features_col
    results = {}
    from itertools import cycle, permutations
    features = list(features) if not isinstance(features, list) else features
    spark = get_spark_session_with_iforest()
    has_features_column = 'features' in df.columns
    if has_features_column:
        row_of_interest = df.select('features').where(
            F.col('is_selected') == True  # noqa
        ).collect()[0].features
    else:
        row_of_interest = df.select(*features).where(
            F.col('is_selected') == True  # noqa
        ).collect()[0]
    print('Row of interest:', row_of_interest)
    random_feature_permutations = cycle(permutations([f for f in features]))
    feat_perm_for_all_rows = []
    # add ids:
    idx_df = df.rdd.zipWithIndex().toDF()
    if has_features_column:
        col_name = f'_1.features'
        idx_df = idx_df.withColumn('features', F.col(col_name))
    else:
        for f in features:
            col_name = f'_1.{f}'
            idx_df = idx_df.withColumn(f, F.col(col_name))
    idx_df = idx_df.withColumn('id', F.col('_2'))
    idx_df = idx_df.withColumn('is_selected', F.col('_1.is_selected'))
    idx_df = idx_df.drop('_1', '_2')
    # get num rows - id starts from 0
    r = idx_df.select(F.max('id').alias('max_id')).collect()[0]
    num_rows = r.max_id + 1
    print('# of Rows:', num_rows)

    # get all permutations - todo:
    # WIP: permutations could be done like this:
    # idx_df = idx_df.withColumn(
    #   'feat_names', F.array(*[F.lit(f) for f in features])
    # )
    # idx_df = idx_df.withColumn('feat_perm', F.shuffle('feat_names'))
    # idx_df.select('feat_names', 'feat_perm').show(2, False)
    for i in range(0, num_rows + 1):
        feat_perm_for_all_rows.append(next(random_feature_permutations))
    shuffle(feat_perm_for_all_rows)

    # set broadcasts
    FEATURES_PERM_BROADCAST = spark.sparkContext.broadcast(
        feat_perm_for_all_rows
    )
    ORDERED_FEATURES_BROADCAST = spark.sparkContext.broadcast(features)
    ROW_OF_INTEREST_BROADCAST = spark.sparkContext.broadcast(row_of_interest)

    if not has_features_column:
        idx_df = idx_df.withColumn('features', F.array(*features))

    # todo: use one udf to calculate both -j +j and explode afterwards
    def calculate_x_minus_j(rid, feature_j, z_features):
        """
        The instance  x−j is the same as  x+j, but in addition
        has feature j replaced by the value for feature j from the sample z
        """
        x_interest = ROW_OF_INTEREST_BROADCAST.value
        curr_feature_perm = list(FEATURES_PERM_BROADCAST.value[rid])
        ordered_features = list(ORDERED_FEATURES_BROADCAST.value)
        x_minus_j = list(z_features).copy()
        f_i = curr_feature_perm.index(feature_j)
        start = f_i + 1 if f_i < len(ordered_features)-1 else -1
        if start >= 0:
            # replace z feature values with x of interest feature values
            # iterate features in current permutation until one before j
            # x-j = [z1, z2, ... zj-1, xj, xj+1, ..., xN]
            # we already have zs so replace z_features with x of interest
            for f in curr_feature_perm[f_i:]:
                # get the initial feature index to preserve initial order
                f_index = ordered_features.index(f)
                new_value = x_interest[f_index]
                x_minus_j[f_index] = new_value
        else:
            pass

        return Vectors.dense(x_minus_j)

    def calculate_x_plus_j(rid, feature_j, z_features):
        """
        The instance  x+j is the instance of interest,
        but all values in the order before feature j are
        replaced by feature values from the sample z
        """
        x_interest = ROW_OF_INTEREST_BROADCAST.value
        curr_feature_perm = list(FEATURES_PERM_BROADCAST.value[rid])
        ordered_features = list(ORDERED_FEATURES_BROADCAST.value)
        x_plus_j = list(z_features).copy()
        f_i = curr_feature_perm.index(feature_j)

        for f in curr_feature_perm[f_i:]:
            f_index = ordered_features.index(f)
            x_plus_j[f_index] = x_interest[f_index]

        return Vectors.dense(x_plus_j)

    udf_x_minus_j = F.udf(calculate_x_minus_j, VectorUDT())
    udf_x_plus_j = F.udf(calculate_x_plus_j, VectorUDT())

    temp_df = idx_df.cache()
    for f in features:
        features_col = F.col('features') if has_features_column else F.array(*features)
        x_minus = f'x_m_j'
        x_plus = f'x_p_j'
        temp_df = temp_df.withColumn(x_plus, udf_x_plus_j('id', F.lit(f), features_col))
        temp_df = temp_df.withColumn(x_minus, udf_x_minus_j('id', F.lit(f), features_col))
        # new_cols[f] = (x_plus, x_minus)
        print('Calculating SHAP values for ', f)
        # minus must be first because of lag:
        # F.col('anomalyScore') - F.col('anomalyScore') one row before
        # so that gives us (x+j - x-j)
        tdf = temp_df.selectExpr(
            'id', f'explode(array({x_minus}, {x_plus})) as features'
        ).cache()
        temp_df = temp_df.drop(x_minus, x_plus)
        # print('Number of rows after:', tdf.count())
        if scaler_model:
            tdf = scaler_model.transform(tdf)
        if model_features_col != 'features':
            tdf = tdf.withColumnRenamed('features', model_features_col)
        ptdf = model.transform(tdf)

        ptdf = ptdf.withColumn(
            'marginal_contribution',
            (
                F.col(anomaly_score_col) - F.lag(
                    F.col(anomaly_score_col), 1).over(
                    Window.partitionBy("id").orderBy("id")
                )
            )
        )
        ptdf = ptdf.filter(ptdf.marginal_contribution.isNotNull())
        results[f] = ptdf.select(
            F.avg('marginal_contribution').alias('shap_value')
        ).collect()[0].shap_value
        print(f'Marginal Contribution for Feature: {f} = {results[f]} ')

    ordered_results = sorted(
        results.items(),
        key=operator.itemgetter(1),
        reverse=True
    )
    for k, v in ordered_results:
        print(f'Feature {k} has a shapley value of: {v}')

    return ordered_results


def construct_tree_graph(trees, feature_names):
    import networkx as nx
    feature_names = list(feature_names)
    num_features = len(feature_names)
    nodes = []
    edges = []
    min_key = min(trees)
    max_key = max(trees)
    forest_root = 'forest_root'
    print(min_key, max_key)
    g = nx.Graph()

    g.add_node(forest_root, )
    nodes.append((forest_root, -100, -100, -100., -1, -1, -100, "None"))

    for i in range(min_key, max_key + 1):
        tree_node = f'tree_{i}'
        g.add_node(tree_node)
        nodes.append((tree_node, -i, -1, -1., -1, -1, -1, "None"))
        g.add_edge(tree_node, forest_root)
        edges.append((tree_node, forest_root, 'child'))
        current_tree_details = trees[i].items()
        # iterate the tree nodes:
        # add the k-th node in the graph with the following characteristics:
        # feature-M = feature value, num_instances = x
        for k, v in current_tree_details:
            k_node = f'{tree_node}_node_{k}'
            fi = v['featureIndex']
            v['feature'] = None
            if -1 < fi < num_features:
                feature = feature_names[fi]
                v['feature'] = feature
            print(k_node, str(v))

            # add the k-th node to the graph
            g.add_node(k_node, **v)
            nodes.append((k_node, *v.values()))

        print(g.number_of_nodes())

        # add edges after all nodes are in place
        for k, v in current_tree_details:
            k_node = f'{tree_node}_node_{k}'
            fi = v['featureIndex']
            lc = v['leftChild']
            rc = v['rightChild']
            feature = None
            if -1 < fi < num_features:
                feature = feature_names[fi]
                v['feature'] = feature
            if k == 0:
                # connect first node to the corresponding tree
                g.add_edge(tree_node, k_node)
                edges.append((tree_node, k_node, 'child'))
            if lc > -1:
                g.add_edge(k_node, f'{tree_node}_node_{lc}')
                edges.append(
                    (
                        k_node,
                        f'{tree_node}_node_{lc}',
                        'child'
                        # f'{feature}<{v["featureValue"]}'
                        )
                )
            if rc > -1:
                g.add_edge(k_node, f'{tree_node}_node_{rc}')
                edges.append(
                    (k_node,
                     f'{tree_node}_node_{rc}',
                     'child'
                     # f'{feature}<{v["featureValue"]}'
                     )
                )
        print('G is created.')
    return g, nodes, edges


def get_trees_and_features(nodes_df):
    collected_data = nodes_df.collect()
    trees = defaultdict(dict)
    features = {}
    for row in collected_data:
        trees[row.treeID][row.nodeData.id] = row.nodeData.asDict()
        features[row.nodeData.featureIndex] = True

    return trees, features


def get_shortest_path_for_tree(tree):
    pass


def get_avg_shortest_path_for_forest(g):
    import networkx as nx
    return nx.average_shortest_path_length(g)


def get_shortest_path_for_g(g):
    import networkx as nx
    return nx.shortest_path(
        g, source='root', target='minus_1', weight=None, method='dijkstra'
    )


def draw_graph(g):
    from matplotlib import pyplot as plt
    import networkx as nx
    plt.rcParams["figure.figsize"] = (20, 20)
    nx.draw(g, node_size=1200,
        node_color='lightblue', linewidths=0.25, font_size=10,
        font_weight='bold', with_labels=True)
    # pos=nx.nx_pydot.graphviz_layout(g),
    plt.show()
    print(g)
