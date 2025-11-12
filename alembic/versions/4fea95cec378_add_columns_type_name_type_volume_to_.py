"""Add columns type_name, type_volume to character_assets and corporation_assets

Revision ID: 4fea95cec378
Revises: 8483347ca1ad
Create Date: 2025-11-07 15:19:32.665229

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4fea95cec378'
down_revision: Union[str, Sequence[str], None] = '8483347ca1ad'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('type_volume', sa.Float, nullable=True))
        batch_op.add_column(sa.Column('type_description', sa.String(length=255), nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('type_volume', sa.Float, nullable=True))
        batch_op.add_column(sa.Column('type_description', sa.String(length=255), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('type_volume')
        batch_op.drop_column('type_description')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('type_volume')
        batch_op.drop_column('type_description')

