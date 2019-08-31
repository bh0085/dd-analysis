"""empty message

Revision ID: f5e7e886daec
Revises: 477ebf1b34d1
Create Date: 2019-08-19 19:48:15.287628

"""
from alembic import op
import sqlalchemy as sa

from geoalchemy2 import Geometry
import geoalchemy2


# revision identifiers, used by Alembic.
revision = 'f5e7e886daec'
down_revision = '477ebf1b34d1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    # op.drop_table('spatial_ref_sys')
    op.add_column('umi', sa.Column('umap_xyz', geoalchemy2.types.Geometry(geometry_type='POINTZ'), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('umi', 'umap_xyz')
    # op.create_table('spatial_ref_sys',
    # sa.Column('srid', sa.INTEGER(), autoincrement=False, nullable=False),
    # sa.Column('auth_name', sa.VARCHAR(length=256), autoincrement=False, nullable=True),
    # sa.Column('auth_srid', sa.INTEGER(), autoincrement=False, nullable=True),
    # sa.Column('srtext', sa.VARCHAR(length=2048), autoincrement=False, nullable=True),
    # sa.Column('proj4text', sa.VARCHAR(length=2048), autoincrement=False, nullable=True),
    # sa.CheckConstraint('(srid > 0) AND (srid <= 998999)', name='spatial_ref_sys_srid_check'),
    # sa.PrimaryKeyConstraint('srid', name='spatial_ref_sys_pkey')
    # )
    # ### end Alembic commands ###
