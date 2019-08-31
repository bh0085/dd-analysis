"""empty message

Revision ID: 56cf258fe395
Revises: f5e7e886daec
Create Date: 2019-08-20 15:42:00.986303

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '56cf258fe395'
down_revision = 'f5e7e886daec'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('linkagecell',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('dsid', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['dsid'], ['dataset.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_linkagecell_dsid'), 'linkagecell', ['dsid'], unique=False)
    op.add_column('umi', sa.Column('lcell', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_umi_lcell'), 'umi', ['lcell'], unique=False)
    op.create_foreign_key(None, 'umi', 'linkagecell', ['lcell'], ['id'], ondelete='CASCADE')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'umi', type_='foreignkey')
    op.drop_index(op.f('ix_umi_lcell'), table_name='umi')
    op.drop_column('umi', 'lcell')
    op.drop_index(op.f('ix_linkagecell_dsid'), table_name='linkagecell')
    op.drop_table('linkagecell')
    # ### end Alembic commands ###
