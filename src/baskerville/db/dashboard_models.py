# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from baskerville.db import Base
from baskerville.db.models import utcnow, SerializableMixin
from sqlalchemy import Column, Integer, ForeignKey, DateTime, Enum, String, \
    Boolean, BigInteger, Float, JSON, Text, TEXT
from sqlalchemy.orm import relationship
from passlib.apps import custom_app_context as pwd_context

from baskerville.util.enums import UserCategoryEnum, FeedbackEnum, \
    FeedbackContextTypeEnum


class UserCategory(Base, SerializableMixin):
    __tablename__ = 'user_categories'
    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(Enum(UserCategoryEnum))
    # a user can belong to more than one category
    users = relationship(
        'User', uselist=True, back_populates='category'
    )


class Organization(Base, SerializableMixin):
    __tablename__ = 'organizations'
    id = Column(BigInteger(), primary_key=True, autoincrement=True, unique=True)
    uuid = Column(String(300), primary_key=True, unique=True)
    name = Column(String(200), index=True)
    details = Column(TEXT())
    registered = Column(Boolean(), default=False)
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    updated_at = Column(
        DateTime(timezone=True), nullable=True, onupdate=utcnow()
    )
    users = relationship(
        'User', uselist=False, back_populates='organization'
    )


class User(Base, SerializableMixin):
    __tablename__ = 'users'
    id = Column(BigInteger(), primary_key=True, autoincrement=True, unique=True)
    id_organization = Column(BigInteger(), ForeignKey('organizations.id'))
    id_category = Column(Integer, ForeignKey('user_categories.id'), nullable=False)
    username = Column(String(200), index=True)
    first_name = Column(String(200), index=True)
    last_name = Column(String(200), index=True)
    email = Column(String(256), unique=True, nullable=False)
    password_hash = Column(String(128))
    is_active = Column(Boolean())
    is_gitlab_login = Column(Boolean(), default=False)
    is_admin = Column(Boolean(), default=False)
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    updated_at = Column(
        DateTime(timezone=True), nullable=True, onupdate=utcnow()
    )

    # users * - 1 category
    category = relationship(
        'UserCategory',
        foreign_keys=id_category, back_populates='users'
    )
    organization = relationship(
        'Organization',
        foreign_keys=id_organization, back_populates='users'
    )
    runtimes = relationship(
        'Runtime',
        uselist=False,
        # back_populates='user'
    )

    _remove = ['password_hash']

    def hash_password(self, password):
        self.password_hash = pwd_context.encrypt(password)
        return self.password_hash

    def verify_password(self, password):
        return pwd_context.verify(password, self.password_hash)


class FeedbackContext(Base, SerializableMixin):
    __tablename__ = 'feedback_contexts'
    id = Column(BigInteger, primary_key=True, autoincrement=True, unique=True)
    uuid_organization = Column(String(300), nullable=False)
    reason = Column(Enum(FeedbackContextTypeEnum))
    reason_descr = Column(TEXT())
    start = Column(DateTime(timezone=True))
    stop = Column(DateTime(timezone=True))
    ip_count = Column(Integer)
    notes = Column(TEXT)
    progress_report = Column(TEXT)
    pending = Column(Boolean(), default=True)


class Feedback(Base, SerializableMixin):
    __tablename__ = 'feedback'

    id = Column(BigInteger, primary_key=True, autoincrement=True, unique=True)
    id_feedback_context = Column(BigInteger(), ForeignKey('feedback_contexts.id'), nullable=False)
    id_user = Column(BigInteger(), ForeignKey('users.id'), nullable=False)
    uuid_request_set = Column(TEXT(), nullable=False)
    prediction = Column(Integer, nullable=False)
    score = Column(Float, nullable=False)
    attack_prediction = Column(Float, nullable=False)
    low_rate = Column(Boolean(), nullable=True)
    ip = Column(String, nullable=False)
    target = Column(String, nullable=False)
    features = Column(JSON, nullable=False)
    feedback = Column(Enum(FeedbackEnum))
    start = Column(DateTime(timezone=True), nullable=False)
    stop = Column(DateTime(timezone=True), nullable=False)
    submitted = Column(Boolean(), default=False)
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    updated_at = Column(
        DateTime(timezone=True), nullable=True, onupdate=utcnow()
    )

    user = relationship(
        'User',
        foreign_keys=id_user
    )
    request_set = relationship(
        'RequestSet',
        primaryjoin='foreign(Feedback.uuid_request_set) == remote(RequestSet.uuid_request_set)'
    )
    feedback_context = relationship(
        'FeedbackContext',
        foreign_keys=id_feedback_context
    )


class SubmittedFeedback(Base, SerializableMixin):
    __tablename__ = 'submitted_feedback'

    id = Column(BigInteger, primary_key=True, autoincrement=True, unique=True)
    # not all feedback is part of an attack
    id_context = Column(BigInteger(), ForeignKey('feedback_contexts.id'), nullable=False)
    uuid_organization = Column(String(300), nullable=False)
    uuid_request_set = Column(TEXT(), nullable=False)
    prediction = Column(Integer, nullable=False)
    score = Column(Float, nullable=False)
    attack_prediction = Column(Float, nullable=False)
    low_rate = Column(Boolean(), nullable=True)
    features = Column(JSON, nullable=True)
    feedback = Column(Enum(FeedbackEnum))
    start = Column(DateTime(timezone=True), nullable=True)
    stop = Column(DateTime(timezone=True), nullable=True)
    submitted_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    updated_at = Column(
        DateTime(timezone=True), nullable=True, onupdate=utcnow()
    )

    organization = relationship(
        'Organization',
        primaryjoin='foreign(SubmittedFeedback.uuid_organization) == remote(Organization.uuid)'
    )
    request_set = relationship(
        'RequestSet',
        primaryjoin='foreign(SubmittedFeedback.uuid_request_set) == remote(RequestSet.uuid_request_set)'
    )
    columns = [
        'id',
        'id_context',
        'uuid_organization',
        'uuid_request_set',
        'prediction',
        'score',
        'attack_prediction',
        'low_rate',
        'features',
        'feedback',
        'start',
        'submitted_at',
        'updated_at'
    ]


class Message(Base, SerializableMixin):
    __tablename__ = 'messages'
    id = Column(BigInteger, primary_key=True, autoincrement=True, unique=True)
    id_user = Column(BigInteger(), ForeignKey('users.id'), nullable=True)
    uuid_organization = Column(String(300), nullable=False)
    message = Column(TEXT(), nullable=False)
    severity = Column(String(), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    user = relationship(
        'User',
        foreign_keys=id_user
    )
    organization = relationship(
        'Organization',
        primaryjoin='foreign(Message.uuid_organization) == remote(Organization.uuid)'
    )


class PendingWork(Base, SerializableMixin):
    __tablename__ = 'pending_work'
    id = Column(BigInteger, primary_key=True, autoincrement=True, unique=True)
    id_user = Column(BigInteger(), ForeignKey('users.id'), nullable=False)
    uuid = Column(String(), nullable=False)
    description = Column(TEXT(), nullable=False)
    logs = Column(TEXT(), nullable=True)
    success = Column(Boolean(), nullable=False, default=False)
    pending = Column(Boolean(), nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), server_default=utcnow())
    updated_at = Column(DateTime(timezone=True), server_default=utcnow())
    user = relationship(
        'User',
        foreign_keys=id_user
    )
