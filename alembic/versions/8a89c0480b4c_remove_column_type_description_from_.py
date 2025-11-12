"""Remove column type_description from character_assets and corporation_assets

Revision ID: 8a89c0480b4c
Revises: 933429a3a392
Create Date: 2025-10-30 16:17:38.095865

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8a89c0480b4c'
down_revision: Union[str, Sequence[str], None] = '933429a3a392'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('type_description')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('type_description')

def downgrade():
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('type_description', sa.String(), nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('type_description', sa.String(), nullable=True))