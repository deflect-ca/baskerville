# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from pyspark.sql import functions as F

from baskerville.models.config import BaskervilleConfig
from baskerville.util.helpers import parse_config, get_default_data_path
from baskerville.util.model_interpretation.helpers import load_sample_df, \
    load_anomaly_model, get_spark_session_with_iforest, \
    calculate_shapley_values_for_all_features

if __name__ == '__main__':
    import os

    data_path = get_default_data_path()
    spark = get_spark_session_with_iforest()
    yaml_conf = parse_config(os.path.join(data_path, '..', 'conf', 'baskerville.yaml'))
    conf = BaskervilleConfig(yaml_conf).validate()
    # load test model
    anomaly_model = load_anomaly_model(
        f'{data_path}/models/AnomalyModel__2020_11_12___16_06_TestModel'
    )
    feature_names = anomaly_model.features.keys()

    # get sample dataset
    df = load_sample_df(
        f'{data_path}/../test_dataset',
        feature_names,
        conf.engine
    )
    df = df.withColumn('mid', F.monotonically_increasing_id())

    df.show(10, False)

    df = anomaly_model.prepare_df(df).withColumn(
        'features', F.col('features_values_scaled')
    )
    pred_df = anomaly_model.predict(df)
    df = pred_df.select('mid', 'features', 'prediction')
    row = df.select('mid', 'features').where(F.col('prediction') > 0).first()
    print('Row: ', row)

    df = df.withColumn(
        'is_selected',
        F.when(F.col('mid') == row.mid, F.lit(True)).otherwise(F.lit(False))
    ).drop('prediction')

    shapley_values_for_all_f = calculate_shapley_values_for_all_features(
        df,
        feature_names,
        anomaly_model.iforest_model,
        features_col=anomaly_model.features_vector_scaled,
        # anomaly_score_col=anomaly_model.score_column
    )
    print(shapley_values_for_all_f)
    print('Shapley values for all features:')
    print('-'*20)
    print(*[f'#{i}. {feat} = {value}\n' for i, (feat, value) in enumerate(shapley_values_for_all_f)])
