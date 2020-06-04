"""add banjax bans table

Revision ID: 4c5d9065aee2
Revises:
Create Date: 2019-02-19 13:12:54.127134

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4c5d9065aee2'
down_revision = '0c5cf09f1fc4'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'banjax_bans',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('sync_start', sa.DateTime(timezone=True)),
        sa.Column('sync_stop', sa.DateTime(timezone=True)),
        sa.Column('ip', sa.TEXT(), nullable=False)
    )
    op.add_column('request_sets', sa.Column('id_banjax', sa.Integer))


def downgrade():
    op.drop_table('banjax_bans')
    op.drop_column('request_sets', 'id_banjax')
