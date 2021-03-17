"""add uuid_request_set in request_sets

Revision ID: 88eb5854154f
Revises:
Create Date: 2020-07-07 11:02:39.321300

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '88eb5854154f'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('request_sets', sa.Column('uuid_request_set', sa.TEXT))


def downgrade():
    op.op.drop_column('request_sets', 'uuid_request_set')
