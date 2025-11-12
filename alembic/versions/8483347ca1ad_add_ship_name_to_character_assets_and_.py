"""Add ship_name to character_assets and corporation_assets

Revision ID: 8483347ca1ad
Revises: 8a89c0480b4c
Create Date: 2025-11-03 12:27:57.824403

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8483347ca1ad'
down_revision: Union[str, Sequence[str], None] = '8a89c0480b4c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('ship_name', sa.String(), nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('ship_name', sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('ship_name')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('ship_name')