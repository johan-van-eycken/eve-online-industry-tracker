"""add type_default_volume and type_repackaged_volume to character_assets and corporation_assets

Revision ID: b57060ab86dd
Revises: 4fea95cec378
Create Date: 2025-11-12 13:45:11.809955

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b57060ab86dd'
down_revision: Union[str, Sequence[str], None] = '4fea95cec378'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('type_default_volume', sa.Float, nullable=True))
        batch_op.add_column(sa.Column('type_repackaged_volume', sa.Float, nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('type_default_volume', sa.Float, nullable=True))
        batch_op.add_column(sa.Column('type_repackaged_volume', sa.Float, nullable=True))

def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('type_default_volume')
        batch_op.drop_column('type_repackaged_volume')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('type_default_volume')
        batch_op.drop_column('type_repackaged_volume')
