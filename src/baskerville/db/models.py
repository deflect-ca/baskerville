# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, TEXT, Float,
    JSON, LargeBinary, BigInteger)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import relationship
from sqlalchemy.sql import expression

from baskerville.db import Base
from baskerville.util.helpers import SerializableMixin


LONG_TEXT_LEN = 4294000000


# https://docs.sqlalchemy.org/en/latest/core/compiler.html#utc-timestamp-function
class utcnow(expression.FunctionElement):
    type = DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


class Encryption(Base, SerializableMixin):
    __tablename__ = 'encryption'

    id = Column(BigInteger, primary_key=True)
    keyhash = Column(TEXT())
    comment = Column(TEXT())
    runtimes = relationship('Runtime', back_populates='encryption')


class Runtime(Base, SerializableMixin):
    __tablename__ = 'runtimes'

    id = Column(BigInteger, primary_key=True)
    id_encryption = Column(BigInteger, ForeignKey('encryption.id'))
    id_user = Column(BigInteger, ForeignKey('users.id'))
    start = Column(DateTime(timezone=True))
    stop = Column(DateTime(timezone=True))
    target = Column(TEXT(), nullable=True)
    dt_bucket = Column(Float)
    file_name = Column(TEXT())
    processed = Column(Boolean)
    n_request_sets = Column(Integer)
    comment = Column(TEXT())
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    config = Column(JSON)
    # todo: active features
    # a runtime can have more than one request_set
    request_sets = relationship(
        'RequestSet', uselist=False, back_populates='runtimes'
    )
    # runtimes * - 1 encryption
    encryption = relationship(
        'Encryption',
        foreign_keys=id_encryption, back_populates='runtimes'
    )
    # runtimes * - 1 users
    try:
        from baskerville.db.dashboard_models import User
    except Exception as exp:
        print(exp)
        pass
    user = relationship(
        'User',
        foreign_keys=id_user, back_populates='runtimes'
    )


class RequestSet(Base, SerializableMixin):
    __tablename__ = 'request_sets'

    id = Column(BigInteger, primary_key=True)
    id_runtime = Column(BigInteger, ForeignKey('runtimes.id'), nullable=True)
    uuid_request_set = Column(TEXT())
    target = Column(TEXT())
    target_original = Column(TEXT())
    ip = Column(String(45))
    ip_encrypted = Column(TEXT())
    ip_iv = Column(TEXT())
    ip_tag = Column(TEXT())
    start = Column(DateTime(timezone=True))
    stop = Column(DateTime(timezone=True))
    total_seconds = Column(Float)
    subset_count = Column(Integer)
    num_requests = Column(Integer)
    time_bucket = Column(Integer)
    label = Column(Integer)
    id_attribute = Column(Integer, ForeignKey('attributes.id'), nullable=True)
    id_banjax = Column(Integer, ForeignKey('banjax_bans.id'), nullable=True)
    process_flag = Column(Boolean, default=True)
    prediction = Column(Integer)
    attack_prediction = Column(Integer)
    challenged = Column(Integer)
    challenge_failed = Column(Integer)
    challenge_passed = Column(Integer)
    banned = Column(Integer)
    low_rate_attack = Column(Integer)
    score = Column(Float)
    classifier_score = Column(Float)
    features = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    updated_at = Column(
        DateTime(timezone=True), nullable=True, onupdate=utcnow()
    )
    model_version = Column(Integer, ForeignKey('models.id'))

    runtimes = relationship(
        'Runtime', uselist=False, back_populates='request_sets'
    )
    models = relationship(
        'Model', uselist=False, back_populates='request_sets'
    )
    model_training_sets_link = relationship(
        'Model', secondary='model_training_sets_link',
        back_populates='request_sets'
    )
    attacks = relationship(
        'Attack', secondary='requestset_attack_link',
        back_populates='request_sets'
    )
    attributes = relationship(
        'Attribute', foreign_keys=id_attribute, uselist=False,
        back_populates='request_sets'
    )
    banjax_bans = relationship(
        'BanjaxBan', foreign_keys=id_banjax, uselist=False,
        back_populates='request_sets'
    )

    columns = [
        'uuid_request_set',
        'ip',
        'target',
        'target_original',
        'subset_count',
        'num_requests',
        'start',
        'stop',
        'prediction',
        'attack_prediction',
        'low_rate_attack',
        'challenged',
        'score',
        'classifier_score',
        'label',
        'id_attribute',
        'features',
        'id_runtime',
        'time_bucket',
        'total_seconds',
        'model_version',
        'updated_at'
    ]

    def prediction_str(self):
        from baskerville.util.enums import LabelEnum
        return "malicious" if self.prediction == LabelEnum.malicious \
            else "benign" if self.prediction == LabelEnum.benign else "unknown"

    def __repr__(self):
        return f'<{self.__class__.__name__} from: {self.start} to: {self.stop} ' \
            f'num_requests: {self.num_requests} ' \
            f'created_at: {self.created_at} ' \
            f'prediction: {self.prediction_str()}>'


class Model(Base, SerializableMixin):
    __tablename__ = 'models'

    id = Column(BigInteger, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    features = Column(JSON)
    algorithm = Column(TEXT())
    scaler_type = Column(TEXT())
    parameters = Column(TEXT())
    recall = Column(Float)
    precision = Column(Float)
    f1_score = Column(Float)
    classifier = Column(LargeBinary)
    scaler = Column(LargeBinary)
    host_encoder = Column(LargeBinary)
    n_training = Column(Integer)
    n_testing = Column(Integer)
    notes = Column(TEXT)
    analysis_notebook = Column(TEXT)
    threshold = Column(Float)

    request_sets = relationship(
        'RequestSet', secondary='model_training_sets_link',
        back_populates='models'
    )


class ModelTrainingSetLink(Base, SerializableMixin):
    __tablename__ = 'model_training_sets_link'

    id_request_set = Column(BigInteger,
                            ForeignKey('request_sets.id'), primary_key=True)
    model_version = Column(BigInteger,
                           ForeignKey('models.id'), primary_key=True)


class Attack(Base, SerializableMixin):
    __tablename__ = 'attacks'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    id_misp = Column(BigInteger)
    uuid_org = Column(TEXT())
    date = Column(DateTime(timezone=True))
    start = Column(DateTime(timezone=True))
    stop = Column(DateTime(timezone=True))
    target = Column(TEXT())
    attack_type = Column(TEXT())
    attack_tool = Column(TEXT())
    attack_source = Column(TEXT())
    ip_count = Column(Integer)
    sync_start = Column(DateTime(timezone=True))
    sync_stop = Column(DateTime(timezone=True))
    processed = Column(Integer)
    notes = Column(TEXT)
    progress_report = Column(TEXT)
    analysis_notebook = Column(TEXT)
    dashboard_url = Column(TEXT)
    anomaly_traffic_portion = Column(Float)
    detected_traffic = Column(Float)

    approved = Column(Boolean)
    labeled = Column(Boolean)
    saved_in_cloud = Column(Boolean)

    request_sets = relationship(
        'RequestSet', secondary='requestset_attack_link',
        back_populates='attacks'
    )
    attributes = relationship(
        'Attribute', secondary='attribute_attack_link',
        back_populates='attacks'
    )
    organization = relationship(
        'Organization',
        primaryjoin='foreign(Attack.uuid_org) == remote(Organization.uuid)'
    )


class Attribute(Base, SerializableMixin):
    __tablename__ = 'attributes'

    id = Column(BigInteger, primary_key=True)
    value = Column(TEXT())

    request_sets = relationship(
        'RequestSet', back_populates='attributes'
    )
    attacks = relationship(
        'Attack', secondary='attribute_attack_link',
        back_populates='attributes'
    )


class BanjaxBan(Base, SerializableMixin):
    __tablename__ = 'banjax_bans'

    id = Column(BigInteger, primary_key=True)
    sync_start = Column(DateTime(timezone=True))
    sync_stop = Column(DateTime(timezone=True))
    ip = Column(TEXT())

    request_sets = relationship(
        'RequestSet', back_populates='banjax_bans'
    )


class RequestSetAttackLink(Base, SerializableMixin):
    __tablename__ = 'requestset_attack_link'

    id_request_set = Column(BigInteger,
                            ForeignKey('request_sets.id'), primary_key=True)
    id_attack = Column(BigInteger,
                       ForeignKey('attacks.id'), primary_key=True)


class AttributeAttackLink(Base, SerializableMixin):
    __tablename__ = 'attribute_attack_link'

    id_attack = Column(BigInteger,
                       ForeignKey('attacks.id'), primary_key=True)
    id_attribute = Column(BigInteger,
                          ForeignKey('attributes.id'), primary_key=True)


class Challenge(Base, SerializableMixin):
    __tablename__ = 'challenge'

    id = Column(BigInteger, primary_key=True)
    start = Column(DateTime(timezone=True))
    host = Column(TEXT(), nullable=False)
    expiration = Column(Integer, nullable=True)
    comment = Column(TEXT(), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
