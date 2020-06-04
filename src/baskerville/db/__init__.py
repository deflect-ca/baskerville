# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.db.data_partitioning import get_temporal_partitions

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils import database_exists, create_database

Base = declarative_base()

defaults = {
    'mysql': 'master',
    'postgres': 'postgres',
}

DATA_PARTITION_TABLES = ['request_sets']


def get_table_inheritance_script_for(table_name):
    return f"""
        DO
        $do$
        DECLARE
          table_name text;
          month text;
          target_month  TEXT ARRAY  DEFAULT  ARRAY['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'];
        BEGIN
          FOREACH month IN ARRAY target_month LOOP
            table_name = '{table_name}_' || month;
            EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', table_name);
            EXECUTE format('CREATE TABLE %I (CHECK ( extract(month from created_at) = ' ||
            quote_literal(month) ||' )) INHERITS (public.{table_name})', table_name) USING month;
          END LOOP;
        END;
        $do$;
    """


def get_f_request_sets_insert_by_month():
    return """
        CREATE OR REPLACE FUNCTION insert_by_month_trigger()
        RETURNS TRIGGER AS $$
        DECLARE
          target_month text;
          table_name text;
        BEGIN
            SELECT cast(extract(month from NEW.created_at) AS TEXT) INTO target_month;
            table_name = TG_ARGV[0] || target_month;
            raise notice '% ', table_name;
            EXECUTE 'INSERT INTO ' || table_name || ' VALUES ($1.*)' USING NEW;
            RETURN NULL;
        exception when others then
          raise notice '% %', SQLERRM, SQLSTATE;
          raise notice 'Insert failed ';
          RETURN NULL;
        END;
        $$
        LANGUAGE plpgsql;
    """


def get_before_insert_trigger(table_name):
    return f"""
    DROP TRIGGER IF EXISTS insert_{table_name}_trigger
      ON {table_name};
    CREATE TRIGGER insert_{table_name}_trigger
    BEFORE INSERT ON {table_name}
    FOR EACH ROW EXECUTE PROCEDURE insert_by_month_trigger('public.{table_name}_');
    """


def get_db_connection_str(conf, default_db=False):
    """

    :param conf:
    :return:
    """
    connection_str = '{db_dependent}{user}:{password}@{host}:{port}/{db}'

    db_type = conf.get('type', None)
    if db_type == 'mysql':
        db_dependent = 'mysql+pymysql://'
    elif db_type == 'postgres':
        db_dependent = 'postgresql+psycopg2://'
    else:
        raise NotImplementedError(
            '{} not implemented yet'.format(db_type)
        )

    return connection_str.format(
        db_dependent=db_dependent,
        user=conf.get('user'),
        password=conf.get('password'),
        host=conf.get('host'),
        port=conf.get('port'),
        db=conf.get('name') if not default_db else defaults.get(conf['type'])
    )


def get_jdbc_url(conf):
    """
    Returns the formatted jdbc connection string
    :param DatabaseConfig conf: the database configuration
    :return:
    :rtype: str
    """
    if conf.type == 'mysql':
        return 'jdbc:{}://{}:{}/{}?' \
               'rewriteBatchedStatements=true&' \
               'reWriteBatchedInserts=true'.format(
                   conf.type,
                   conf.host,
                   conf.port,
                   conf.name,
               )
    elif conf.type == 'postgres':
        return 'jdbc:{}://{}:{}/{}?' \
               'user={}&' \
               'password={}&' \
               'rewriteBatchedStatements=true&' \
               'reWriteBatchedInserts=true'.format(
                   'postgresql',
                   conf.host,
                   conf.port,
                   conf.name,
                   conf.user,
                   conf.password
               )


def set_up_db(conf, create=True, partition=True):
    """
    Create database tables and session object
    :param dict conf: the database configuration
    :param bool create: if True, try to create the database, else, assume
    database exists and connect directly to it.
    :return: a session and an engine instance
    :rtype: tuple(session, engine)
    """
    if conf.get('type') == 'postgres':
        if create:
            try:
                # with contextlib.suppress(ProgrammingError) as e:
                with create_engine(
                        get_db_connection_str(conf, default_db=True),
                        isolation_level='AUTOCOMMIT',
                        **conf.get('db_conn_args', {})
                ).connect() as connection:
                    connection.execute(f'CREATE DATABASE {conf.get("name")}')
                    connection.execute(
                        'CREATE CAST (VARCHAR AS JSON) '
                        'WITHOUT FUNCTION AS IMPLICIT'
                    )
            except ProgrammingError:
                pass

        engine = create_engine(
            get_db_connection_str(conf),
            client_encoding='utf8',
            use_batch_mode=True,
            pool_recycle=120,
            **conf.get('db_conn_args', {})
        )

    else:
        engine = create_engine(
            get_db_connection_str(conf),
            pool_recycle=120,
            **conf.get('db_conn_args', {})
        )

        if not database_exists(engine.url):
            create_database(engine.url)

    Session = scoped_session(sessionmaker(bind=engine))
    Base.metadata.create_all(bind=engine)
    # session = Session()

    # create data partition
    maintenance_conf = conf.get('maintenance')
    if conf.get('type') == 'postgres' \
            and maintenance_conf \
            and maintenance_conf['data_partition'] \
            and create \
            and partition:
        Session.execute(text(get_temporal_partitions(maintenance_conf)))
        print('Partitioning done...')

    return Session, engine
