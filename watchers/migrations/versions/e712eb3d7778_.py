"""empty message

Revision ID: e712eb3d7778
Revises: 874b620a0c02
Create Date: 2019-09-27 15:21:13.442788

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2

# revision identifiers, used by Alembic.
revision = 'e712eb3d7778'
down_revision = '874b620a0c02'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('segment', sa.Column('point12', geoalchemy2.types.Geometry(geometry_type='MULTIPOINT'), nullable=True))
    op.add_column('segment', sa.Column('points', geoalchemy2.types.Geometry(geometry_type='MULTIPOINT'), nullable=True))
    op.add_column('segment', sa.Column('points128', geoalchemy2.types.Geometry(geometry_type='MULTIPOINT'), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('segment', 'points128')
    op.drop_column('segment', 'points')
    op.drop_column('segment', 'point12')
    # ### end Alembic commands ###
