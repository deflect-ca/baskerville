"""remove subsets

Revision ID: 41ff5ba653c6
Revises: 4c5d9065aee2
Create Date: 2019-03-04 14:11:26.543111

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
from baskerville.db.models import utcnow

revision = '41ff5ba653c6'
down_revision = '4c5d9065aee2'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table('subsets')


def downgrade():
    op.create_table(
        'subsets',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('target', sa.String(45), nullable=False),
        sa.Column('ip', sa.TEXT(), nullable=False),
        sa.Column('start', sa.DateTime(timezone=True)),
        sa.Column('stop', sa.DateTime(timezone=True)),
        sa.Column('num_requests', sa.Integer(), nullable=False),
        sa.Column('features', sa.JSON()),
        sa.Column('prediction', sa.Integer()),
        sa.Column('row_num', sa.Integer()),
        sa.Column('r', sa.Float()),
        sa.Column('time_bucket', sa.Integer()),
        sa.Column(
            'created_at', sa.DateTime(timezone=True), server_default=utcnow()
        ),
    )
