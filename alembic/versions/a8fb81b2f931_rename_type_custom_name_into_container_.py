"""Rename type_custom_name into container_name for character_assets and corporation_assets

Revision ID: a8fb81b2f931
Revises: 
Create Date: 2025-10-30 11:22:43.734660

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a8fb81b2f931'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.alter_column('type_custom_name', new_column_name='container_name')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.alter_column('type_custom_name', new_column_name='container_name')


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.alter_column('container_name', new_column_name='type_custom_name')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.alter_column('container_name', new_column_name='type_custom_name')
