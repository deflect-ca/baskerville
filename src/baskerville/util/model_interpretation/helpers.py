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
from pyspark.sql import functions as F, types as T
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
        model_features_col: str = 'features',
        column_to_examine: str = 'anomalyScore',
        use_absolute: bool = False
):
    """
    # https://christophm.github.io/interpretable-ml-book/shapley.html#estimating-the-shapley-value
    #  "First, select an instance of interest x, a feature j and the number of
    #  iterations M. For each iteration, a random instance z is
    #  selected from the data and a random order of the features is generated.
    #  Two new instances are created by combining values from #  the instance
    #  of interest x and the sample z. The instance  x+j is the instance of
    #  interest, but all values in the order before feature j are replaced by
    #  feature values from the sample z. The instance x−j is the same as x+j,
    #  but in addition has feature j #  replaced by the value for feature j
    #  from the sample z. The difference in the prediction from the black
    #  box is computed"
    """
    results = {}
    if not df.is_cached:
        df = df.persist()
    features = list(features) if not isinstance(features, list) else features
    spark = get_spark_session_with_iforest()

    # get row of interest
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
    ROW_OF_INTEREST_BROADCAST = spark.sparkContext.broadcast(row_of_interest)

    # get permutations
    feat_df = df.withColumn(
        'ordered_features', F.array(*[F.lit(f) for f in features])
    ).withColumn(
        'features_perm', F.shuffle('ordered_features')
    )

    def calculate_x(feature_j, z_features, curr_feature_perm, ordered_features):
        """
        The instance  x+j is the instance of interest,
        but all values in the order before feature j are
        replaced by feature values from the sample z
        The instance  x−j is the same as  x+j, but in addition
        has feature j replaced by the value for feature j from the sample z
        """
        x_interest = ROW_OF_INTEREST_BROADCAST.value
        # curr_feature_perm = list(FEATURES_PERM_BROADCAST.value[rid])
        # ordered_features = list(ORDERED_FEATURES_BROADCAST.value)
        x_minus_j = list(z_features).copy()
        x_plus_j = list(z_features).copy()
        f_i = curr_feature_perm.index(feature_j)
        after_j = False
        for f in curr_feature_perm[f_i:]:
            # replace z feature values with x of interest feature values
            # iterate features in current permutation until one before j
            # x-j = [z1, z2, ... zj-1, xj, xj+1, ..., xN]
            # we already have zs because we go row by row with the udf,
            # so replace z_features with x of interest
            f_index = ordered_features.index(f)
            new_value = x_interest[f_index]
            x_plus_j[f_index] = new_value
            if after_j:
                x_minus_j[f_index] = new_value
            after_j = True

        # minus must be first because of lag
        return Vectors.dense(x_minus_j), Vectors.dense(x_plus_j)

    udf_calculate_x = F.udf(calculate_x, T.ArrayType(VectorUDT()))

    # persist before processing
    feat_df = feat_df.persist()

    for f in features:
        # use features col if exists or gather features in an array
        features_curr_column = F.col('features') if has_features_column \
            else F.array(*features)
        # x contains x-j and x+j
        feat_df = feat_df.withColumn('x', udf_calculate_x(
            F.lit(f), features_curr_column, 'features_perm', 'ordered_features'
        )).persist()
        print(f'Calculating SHAP values for "{f}"...')
        # minus must be first because of lag:
        # F.col('anomalyScore') - F.col('anomalyScore') one row before
        # so we should have:
        # [x-j row i,  x+j row i]
        # [x-j row i+1,  x+j row i+1]
        # ...
        # that with explode becomes:
        # x-j row i
        # x+j row i
        # x-j row i+1
        # x+j row i+1
        # so that with lag it gives us (x+j - x-j)
        tdf = feat_df.selectExpr(
            'id', f'explode(x) as {model_features_col}'
        ).cache()
        # drop column to release memory
        # temp_df = temp_df.drop('x')
        predict_df = model.transform(tdf)

        predict_df = predict_df.withColumn(
            'marginal_contribution',
            (
                    F.col(column_to_examine) - F.lag(
                    F.col(column_to_examine), 1).over(
                    Window.partitionBy("id").orderBy("id")
                )
            )
        )
        predict_df = predict_df.filter(
            predict_df.marginal_contribution.isNotNull()
        )

        # calculate the average and use the absolute values if asked.
        marginal_contribution_filter = F.avg('marginal_contribution')
        if use_absolute:
            marginal_contribution_filter = F.abs(marginal_contribution_filter)
        marginal_contribution_filter = marginal_contribution_filter.alias(
            'shap_value'
        )

        results[f] = predict_df.select(
            marginal_contribution_filter
        ).first().shap_value
        tdf.unpersist()
        tdf = None
        del tdf
        print(f'Marginal Contribution for feature: {f} = {results[f]} ')

    ordered_results = sorted(
        results.items(),
        key=operator.itemgetter(1),
        reverse=True
    )
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
