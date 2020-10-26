"""Added id_client in Runtime's table

Revision ID: c543e79c0a80
Revises: 88eb5854154f
Create Date: 2020-10-24 12:50:04.152830

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c543e79c0a80'
down_revision = '88eb5854154f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('runtimes', sa.Column('id_client', sa.TEXT))


def downgrade():
    op.op.drop_column('runtimes', 'id_client')
