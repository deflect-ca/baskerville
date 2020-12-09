# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from collections import OrderedDict

from typing import List

from baskerville.util.model_interpretation.helpers import \
    get_spark_session_with_iforest, get_sample_labeled_data, \
    gather_features_with_assembler, RANDOM_SEED, calculate_shapley_values_for_all_features
from pyspark.sql import functions as F
from pyspark.ml.feature import StandardScaler
from pyspark_iforest.ml.iforest import IForest, IForestModel

from affirm.model_interpretation.shparkley.estimator_interface import (
    OrderedSet, ShparkleyModel
)

if __name__ == '__main__':
    spark = get_spark_session_with_iforest()
    data = get_sample_labeled_data()
    df = spark.createDataFrame(data)
    df = df.withColumn('mid', F.monotonically_increasing_id())
    feature_names = OrderedSet(list(data[0].keys()))
    feature_names.remove('label')
    df = gather_features_with_assembler(df, feature_names)
    # df.show()

    scaler = StandardScaler(inputCol='features', outputCol='scaledFeatures')
    iforest = IForest(contamination=0.3, maxDepth=2)
    iforest.setSeed(RANDOM_SEED)  # for reproducibility

    df_no_label = df.select(*feature_names)
    scaler_model = scaler.fit(df.select('features'))
    df_scaled = scaler_model.transform(df)
    df_scaled = df_scaled.withColumn('features', F.col('scaledFeatures')).drop(
        'scaledFeatures')

    # get the scaled features as columns not as dense vector:
    dc = df_scaled.rdd.map(lambda x:[float(y) for y in x['features']]).toDF(list(feature_names))
    dc = dc.withColumn('mid', F.monotonically_increasing_id())
    iforest_model = iforest.fit(df_scaled)
    # print('scaled:', '-'* 20)
    # df_scaled.show()
    # print('dc:', '-'* 20)
    # dc.show()


    class IForestShparkleyModel(ShparkleyModel):
        """
        You need to wrap your model with this interface (by subclassing ShparkleyModel)
        """
        def __init__(self, model: IForestModel, required_features: OrderedSet):
            super().__init__(model)
            self._model = model
            self._required_features = required_features
            self.session = get_spark_session_with_iforest()

        def predict(self, feature_matrix: List[OrderedDict]) -> List[float]:
            """
            Generates one prediction per row, taking in a list of ordered
            dictionaries (one per row).
            """
            df = self.session.createDataFrame(feature_matrix)
            df = df.withColumn(self._model.featureCol, F.array(*self.get_required_features()))
            df.show(10, False)
            preds = self._model.transform(df.select(self._model.featureCol)).toDF()
            print(preds)
            print(preds.count())
            print(preds.head)
            return preds

        def _get_required_features(self) -> OrderedSet:
            """
            An ordered set of feature column names
            """
            return self._required_features


    row = dc.rdd.first()
    print('row:', row)
    shparkley_wrapped_model = IForestShparkleyModel(iforest_model, feature_names)

    # You need to sample your dataset based on convergence criteria.
    # More samples results in more accurate shapley values.
    # Repartitioning and caching the sampled dataframe will speed up computation.
    sampled_df = dc.sample(False, fraction=0.7)
    # print('sampled_df:', sampled_df)
    # sampled_df.show()
    df_ = dc.withColumn(
        'is_selected',
        F.when(F.col('mid')==row.mid, F.lit(True)).otherwise(F.lit(False))
    )

    shapley_values_for_all_f = calculate_shapley_values_for_all_features(
        df_, feature_names, iforest_model
    )
    print(shapley_values_for_all_f)
    print('Shapley values for all features:')
    print('-'*20)
    print(*[f'#{i}. {feat} = {value}\n' for i, (feat, value) in enumerate(shapley_values_for_all_f)])
    # print(zip(shapley_values_for_all_f.keys(), shapley_values_for_all_f.values()))

    # shapley_scores_by_feature = compute_shapley_for_sample(
    #     df=sampled_df,
    #     model=shparkley_wrapped_model,
    #     row_to_investigate=row,
    #     weight_col_name=None
    # )