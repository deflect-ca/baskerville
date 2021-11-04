# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import os
import traceback
from datetime import datetime, timedelta

import isoweek
from pyaml_env import parse_config
from baskerville.db import get_temporal_partitions, set_up_db
from baskerville.db.data_archive import get_archive_script
from baskerville.models.config import DatabaseConfig
from baskerville.util.helpers import get_logger, get_days_in_year
from sqlalchemy.exc import SQLAlchemyError


def maintain_db():
    """
    Runs the partitioning and archive scripts
    :return:
    """
    # todo: this can fail silently
    baskerville_root = os.environ.get(
        'BASKERVILLE_ROOT', '../../../../baskerville'
    )
    # we need the current config for the database details
    config = parse_config(path=f'{baskerville_root}/conf/baskerville.yaml')
    logger = get_logger(
        __name__,
        logging_level=config['engine']['log_level'],
        output_file=config['engine']['logpath']
    )
    db_config = DatabaseConfig(config['database']).validate()

    if db_config.maintenance.partition_by != 'week':
        raise NotImplementedError(
            f'Partition by {db_config.maintenance.partition_by} '
            f'is not yet implemented'
        )

    # maintainance will run every Sunday, so now should be Sunday night
    # move to the start of Monday
    now = datetime.utcnow()
    y, w, _ = now.isocalendar()
    partition_start_week = isoweek.Week(y, w + 1)
    start = datetime.combine(
        partition_start_week.monday(), datetime.min.time()
    )
    end = datetime.combine(
        partition_start_week.sunday(), datetime.max.time()
    )

    logger.info(f'Data Partition Start : {start}')

    diy = get_days_in_year(end.year)
    latest_archive_date = end - timedelta(days=diy)
    latest_archive_year, latest_archive_week, _ = latest_archive_date.isocalendar()
    print(latest_archive_week, latest_archive_year)

    if latest_archive_week > 1:
        latest_archive_week = latest_archive_week - 1
    else:
        latest_archive_week = isoweek.Week.last_week_of_year(
            latest_archive_year-1
        ).week
        latest_archive_year = latest_archive_year - 1
    week = isoweek.Week(latest_archive_year, latest_archive_week)

    print(week)

    db_config.maintenance.data_partition.since = start
    db_config.maintenance.data_partition.until = (
        start + timedelta(days=6)
    ).replace(
        hour=23, minute=59, second=59
    )

    db_config.maintenance.data_archive.since = datetime.combine(
        week.monday(), datetime.min.time()
    )
    db_config.maintenance.data_archive.until = datetime.combine(
        week.sunday(), datetime.max.time()
    )

    print(db_config.maintenance.data_partition)
    print(db_config.maintenance.data_archive)

    # get sql scripts
    partition_sql = get_temporal_partitions(db_config.maintenance)

    archive_sql = get_archive_script(
        latest_archive_date - timedelta(weeks=1),
        latest_archive_date
    )

    logger.debug(partition_sql)
    logger.debug(archive_sql)
    session, engine = set_up_db(db_config.__dict__, create=False)

    try:
        # create partitions
        session.execute(partition_sql)
        session.commit()
        print('Partitioning done')
        # detach partitions over a year and attach them to the archive table
        session.execute(archive_sql)
        session.commit()
        print('Archive done')

    except SQLAlchemyError as e:
        traceback.print_exc()
        session.rollback()
        logger.error(f'Error executing maintenance: {e}')
    finally:
        session.close()


if __name__ == '__main__':
    maintain_db()
