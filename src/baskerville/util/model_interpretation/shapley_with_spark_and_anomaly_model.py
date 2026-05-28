# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import time
import warnings

from pyspark.sql import functions as F
import dateutil.parser

from baskerville.util.model_interpretation.helpers import load_dataframe, \
    load_anomaly_model, get_spark_session_with_iforest, \
    calculate_shapley_values_for_all_features, select_row, \
    load_dataframe_from_db


def shapley_values_for_anomaly_model(
        model_path,
        row_id,
        data_path=None,
        start=None,
        stop=None,
        use_absolute=False,
        column_to_examine=None
):
    _ = get_spark_session_with_iforest()
    anomaly_model = load_anomaly_model(model_path)
    feature_names = anomaly_model.features.keys()

    # get sample dataset
    if data_path:
        df = load_dataframe(data_path, feature_names)
    else:
        # todo: needs database configuration
        df = load_dataframe_from_db(start, stop)
    if 'id' not in df.columns:
        # not a good idea
        warnings.warn('Missing column "id", using monotonically_increasing_id')
        df = df.withColumn('id', F.monotonically_increasing_id())
    df.show(10, False)

    # rename features to the expected column name
    df = anomaly_model.prepare_df(df).withColumn(
        'features', F.col(anomaly_model.features_vector_scaled)
    )
    # predict on the data
    pred_df = anomaly_model.predict(df)
    df = pred_df.select('id', 'features', 'prediction')

    # select the row to be examined
    df = select_row(df, row_id)
    row = df.select('id', 'features').where(
        F.col('is_selected') is True
    ).first()
    print('Row: ', row)

    df = df.drop('prediction')

    shapley_values_for_all_f = calculate_shapley_values_for_all_features(
        df,
        feature_names,
        anomaly_model.iforest_model,
        model_features_col=anomaly_model.features_vector_scaled,
        column_to_examine=column_to_examine,
        use_absolute=use_absolute
    )
    print('Shapley values for all features:')
    print('-' * 20)
    print(*[f'#{i}. {feat} = {value}\n' for i, (feat, value) in
            enumerate(shapley_values_for_all_f)])
    return shapley_values_for_all_f


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'modelpath', help='The path to the model to be examined'
    )
    parser.add_argument(
        'id',
        help='The request_set id to be examined',
        type=str
    )
    parser.add_argument(
        '-p', '--datapath', help='The path to the data'
    )
    parser.add_argument(
        '-s', '--start', help='Date used to filter stop field',
        type=dateutil.parser.parse
    )
    parser.add_argument(
        '-u', '--stop', help='Date used to filter stop field',
        type=dateutil.parser.parse
    )
    parser.add_argument(
        '-a', '--absolute', help='Use absolute values', action='store_true',
    )
    parser.add_argument(
        '-c', '--column',
        help='Column to examine, e.g. prediction. '
             'Default is "anomalyScore"',
        default='anomalyScore',
        type=str
    )
    args = parser.parse_args()
    model_path = args.modelpath
    data_path = args.datapath
    use_absolute = args.absolute
    column_to_examine = args.column
    row_id = args.id
    start = args.start
    stop = args.stop

    if not os.path.isdir(model_path):
        raise ValueError('Model path does not exist')
    if not os.path.isdir(data_path) and not (start and stop):
        raise ValueError(
            'Data path does not exist and no start and stop is set'
        )
    if start and stop and start > stop:
        raise ValueError('Start date is greater than stop date.')
    start = time.time()
    print('Start:', time.time())
    shapley_values_for_anomaly_model(
        model_path,
        row_id,
        data_path,
        start,
        stop,
        use_absolute=use_absolute,
        column_to_examine=column_to_examine
    )
    end = time.time()
    print('End:', time.time())
    print(f'Calculation took {int(end - start)} seconds')
