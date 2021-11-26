# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import pytz
from baskerville.features.helpers import update_features
from baskerville.features.helpers import extract_features_in_order
from baskerville.spark.schemas import cross_reference_schema
from baskerville.util.enums import LabelEnum
from dateutil.tz import tzutc
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import functions as F
from pyspark.sql import types as T
import numpy as np


def remove_www(host):
    if host[:4] == 'www.':
        host = host[4:]
    host = host.split(':')[0]
    host = host.lower()
    return host


def normalize_host_name(host):
    if host[:4] == 'www.':
        host = host[4:]
    h_split = host.replace(':', '.').split('.')
    host_norm = max(h_split, key=len)
    if len(set([h for h in h_split if len(h) == len(host_norm)])) > 1:
        host_norm = host
    host_norm = host_norm.lower()

    return host_norm


def compute_geotime(lat, lon, t, feature_default):
    """
    Given a latitude and a longitude for a (utc) timestamp, calculate
    the local time.
    :param float lat: latitude
    :param float lon: longitude
    :param datetime timestamp: the utc timestamp
    :param float feature_default: the default value
    :return:
    """
    if not lat and not lon:
        # todo: how do latitude/longitude appear in raw ats record?
        feature_value = feature_default
    else:
        from tzwhere import tzwhere
        tz = tzwhere.tzwhere()
        timezone_str = tz.tzNameAt(lat, lon)
        t = t.astimezone(pytz.timezone(timezone_str))

        # t += timezone.utcoffset(t)
        feature_value = (float(t.hour) + float(t.minute) / 60.) % 24.

    return feature_value


def predict_dict(scaler, classifier, model_features, dict_features):
    # commented out to remove np dependency
    return 0, 0
    # """
    # Scale the feature values and use the model to predict
    # :param dict[str, float] dict_features: the feature dictionary
    # :return: 0 if normal, 1 if abnormal, -1 if something went wrong
    # """
    # import numpy as np
    # import pickle
    # import json
    #
    # if isinstance(dict_features, str):
    #     dict_features = json.loads(dict_features)
    #
    # x_test = extract_features_in_order(dict_features, model_features)
    # scaler = pickle.loads(scaler)
    # clf = pickle.loads(classifier)
    # try:
    #     x_scaled = scaler.transform(x_test)
    #     y = clf.decision_function(x_scaled)
    #     prediction = np.sign(y)[0]
    #     r = np.float32(np.absolute(y)[0])
    #     return float(prediction), float(r)
    # except ValueError:
    #     # traceback.print_exc()
    #     print('Cannot predict:', x_test)
    #     return 0, 0


def delete_by(past_to_current_ids, db_conf):
    """
    Delete the old request_sets (that refer to the same target ip pairs as the
    current request_sets)
    :param tuple[int, int] past_to_current_ids:
    :param str db_conf: the serialized db_conf
    :return: True if everything went well, False otherwise
    :rtype: bool
    """
    import json
    from baskerville.db import set_up_db
    from baskerville.db.models import RequestSet

    db_conf = json.loads(db_conf)
    session, engine = set_up_db(db_conf, False)

    try:
        session.query(RequestSet).filter(
            RequestSet.id.in_([x[0] for x in past_to_current_ids])
        ).delete(synchronize_session=False)
        session.commit()
    except Exception:
        import traceback
        traceback.print_exc()
        session.rollback()  # do we want to rollback here?
        session.close()
        engine.dispose()
        return False

    session.close()
    engine.dispose()

    return True


def update_request_set_by_id(
        id, stop, features, prediction, subset_count, num_requests, db_conf
):
    """
    Updates a RequestSet by id
    :param int id: the request_set id
    :param datetime.datetime stop: current subset's stop datetime
    :param dict features: latest features
    :param float prediction: the current prediction
    :param int subset_count: the subset count
    :param int num_requests: the current subset's num_requests
    :param str db_conf: the database configuration serialized
    :return:
    """
    import json
    import traceback
    from baskerville.db import set_up_db
    from baskerville.db.models import RequestSet

    db_conf = json.loads(db_conf)
    session, engine = set_up_db(db_conf, False)

    stop = stop.replace(tzinfo=tzutc())
    values = {
        "stop": stop,
        "features": features,
        "prediction": prediction,
        "subset_count": subset_count,
        "num_requests": num_requests,
    }
    try:
        # with engine.connect() as conn:
        #     stmt = RequestSet.__table__.update(). \
        #         values(values).where(RequestSet.id == id)
        #     conn.execute(stmt)
        #
        # session.bulk_update_mappings(RequestSet, [values])
        session.query(RequestSet).filter(
            RequestSet.id == id
        ).update(values)
        session.flush()
        session.commit()
    except Exception:
        traceback.print_exc()
        session.rollback()
        session.close()
        engine.dispose()
        return False

    session.close()
    engine.dispose()

    return True


def bulk_update_request_sets(
        id, stop, features, prediction, subset_count, num_requests,
        total_seconds, db_conf
):
    import json
    import traceback
    from itertools import zip_longest
    from baskerville.db import set_up_db
    from baskerville.db.models import RequestSet

    db_conf = json.loads(db_conf)

    # https://stackoverflow.com/questions/13125236/sqlalchemy-psycopg2-and-postgresql-copy
    # https://www.endpoint.com/blog/2014/04/11/speeding-up-saving-millions-of-orm

    session, engine = set_up_db(db_conf, False)
    req_per_row = list(
        zip_longest(id, stop, features, prediction, subset_count, num_requests,
                    total_seconds)
    )
    try:
        for request_set in req_per_row:
            values = {
                "stop": request_set[1],
                "features": request_set[2],
                "prediction": request_set[3],
                "subset_count": request_set[4],
                "num_requests": request_set[5],
                "total_seconds": request_set[6]
            }
            session.query(RequestSet).filter(
                RequestSet.id == request_set[0]
            ).update(values)
        session.flush()
        session.commit()
    except Exception:
        traceback.print_exc()
        session.rollback()
        session.close()
        engine.dispose()
        return False

    session.close()
    engine.dispose()

    return True


def cross_reference_misp(ip, db_conf):
    import json
    from baskerville.db import set_up_db
    from baskerville.db.models import Attribute

    db_conf = json.loads(db_conf)
    session, engine = set_up_db(db_conf, False)

    attribute = session.query(Attribute).filter(
        Attribute.value == ip).first()

    label = None
    id_attribute = None
    if attribute:
        label = LabelEnum.malicious.value
        id_attribute = attribute.id

    session.close()
    engine.dispose()

    return label, id_attribute


def get_msg(row, cmd_name):
    """
    Constructs kafka message depending on cmd_name
    """
    import json
    if 'challenge_' in cmd_name:
        return json.dumps(
            {'name': cmd_name, 'value': row}
        ).encode('utf-8')
    elif cmd_name == 'prediction_center' or 'feedback_center':
        return json.dumps(row.asDict()).encode('utf-8')


def send_to_kafka(
        kafka_servers,
        topic,
        rows,
        cmd_name='challenge_host',
        id_client=None,
        client_only=False,
):
    """
    Creates a kafka producer and sends the rows one by one,
    along with the specified command (challenge_[host, ip])
    :returns: False if something went wrong, true otherwise
    """
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
        )
        for row in rows:
            message = get_msg(row, cmd_name)
            if not client_only:
                producer.send(topic, get_msg(row, cmd_name))
            if id_client:
                producer.send(f'{topic}.{id_client}', message)
        producer.flush()
    except Exception:
        import traceback
        traceback.print_exc()
        return False
    return True


def get_msg_from_columns(row, columns):
    import json
    return json.dumps(dict((k, row[k]) for k in columns)).encode('utf-8')


prediction_schema = T.StructType([
    T.StructField("prediction", T.FloatType(), False),
    T.StructField("r", T.FloatType(), False)
])

udf_normalize_host_name = F.udf(normalize_host_name, T.StringType())
udf_remove_www = F.udf(remove_www, T.StringType())
udf_predict_dict = F.udf(predict_dict, prediction_schema)
udf_compute_geotime = F.udf(compute_geotime, T.FloatType())
udf_delete_by = F.udf(delete_by, T.BooleanType())
udf_update_request_set_by_id = F.udf(update_request_set_by_id, T.BooleanType())
udf_cross_reference_misp = F.udf(cross_reference_misp, cross_reference_schema)
udf_update_features = F.udf(
    update_features, T.MapType(T.StringType(), T.FloatType())
)
udf_bulk_update_request_sets = F.udf(bulk_update_request_sets, T.BooleanType())
udf_to_dense_vector = F.udf(lambda l: Vectors.dense(l), VectorUDT())
udf_add_to_dense_vector = F.udf(lambda features, arr: Vectors.dense(np.append(features, [v for v in arr])), VectorUDT())
udf_send_to_kafka = F.udf(send_to_kafka, T.BooleanType())
