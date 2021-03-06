"""empty message

Revision ID: 13f59abc09af
Revises: 56cf258fe395
Create Date: 2019-08-20 17:25:51.680539

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '13f59abc09af'
down_revision = '56cf258fe395'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('linkagecell', sa.Column('center', geoalchemy2.types.Geometry(geometry_type='POINT'), nullable=True))
    op.add_column('linkagecell', sa.Column('hull', geoalchemy2.types.Geometry(geometry_type='POLYGON'), nullable=True))
    op.add_column('linkagecell', sa.Column('kde_density', geoalchemy2.types.Raster(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('linkagecell', 'kde_density')
    op.drop_column('linkagecell', 'hull')
    op.drop_column('linkagecell', 'center')
    # ### end Alembic commands ###
