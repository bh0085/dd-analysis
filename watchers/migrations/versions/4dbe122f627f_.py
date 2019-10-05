"""empty message

Revision ID: 4dbe122f627f
Revises: 4f45f4d46c69
Create Date: 2019-09-15 21:34:27.045408

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '4dbe122f627f'
down_revision = '4f45f4d46c69'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dataset', sa.Column('raster_2k_all', geoalchemy2.types.Raster(), nullable=True))
    op.add_column('dataset', sa.Column('raster_2k_blue', geoalchemy2.types.Raster(), nullable=True))
    op.add_column('dataset', sa.Column('raster_2k_green', geoalchemy2.types.Raster(), nullable=True))
    op.add_column('dataset', sa.Column('raster_2k_red', geoalchemy2.types.Raster(), nullable=True))
    op.add_column('dataset', sa.Column('raster_400_all', geoalchemy2.types.Raster(), nullable=True))
    op.add_column('dataset', sa.Column('raster_400_blue', geoalchemy2.types.Raster(), nullable=True))
    op.add_column('dataset', sa.Column('raster_400_green', geoalchemy2.types.Raster(), nullable=True))
    op.add_column('dataset', sa.Column('raster_400_red', geoalchemy2.types.Raster(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('dataset', 'raster_400_red')
    op.drop_column('dataset', 'raster_400_green')
    op.drop_column('dataset', 'raster_400_blue')
    op.drop_column('dataset', 'raster_400_all')
    op.drop_column('dataset', 'raster_2k_red')
    op.drop_column('dataset', 'raster_2k_green')
    op.drop_column('dataset', 'raster_2k_blue')
    op.drop_column('dataset', 'raster_2k_all')
    # ### end Alembic commands ###
