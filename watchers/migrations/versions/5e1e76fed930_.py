"""empty message

Revision ID: 5e1e76fed930
Revises: 674c3f878d96
Create Date: 2019-09-15 19:00:05.569348

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '5e1e76fed930'
down_revision = '674c3f878d96'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('segment', sa.Column('hull1', geoalchemy2.types.Geometry(geometry_type='POLYGON'), nullable=True))
    op.add_column('segment', sa.Column('hull128', geoalchemy2.types.Geometry(geometry_type='POLYGON'), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('segment', 'hull128')
    op.drop_column('segment', 'hull1')
    # ### end Alembic commands ###
