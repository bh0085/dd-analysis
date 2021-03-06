"""empty message

Revision ID: 30599e358883
Revises: 5e1e76fed930
Create Date: 2019-09-15 19:22:23.624016

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '30599e358883'
down_revision = '5e1e76fed930'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('segment', sa.Column('area12', sa.Numeric(), nullable=True))
    op.add_column('segment', sa.Column('hull12', geoalchemy2.types.Geometry(geometry_type='POLYGON'), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('segment', 'hull12')
    op.drop_column('segment', 'area12')
    # ### end Alembic commands ###
