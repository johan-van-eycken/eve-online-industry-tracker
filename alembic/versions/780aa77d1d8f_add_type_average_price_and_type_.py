"""Add type_average_price and type_adjusted_price to character_assets and corporation_assets

Revision ID: 780aa77d1d8f
Revises: 8c1541ca18c0
Create Date: 2025-11-12 16:26:35.699397

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '780aa77d1d8f'
down_revision: Union[str, Sequence[str], None] = '8c1541ca18c0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('type_average_price', sa.Float, nullable=True))
        batch_op.add_column(sa.Column('type_adjusted_price', sa.Float, nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('type_average_price', sa.Float, nullable=True))
        batch_op.add_column(sa.Column('type_adjusted_price', sa.Float, nullable=True))

def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('type_average_price')
        batch_op.drop_column('type_adjusted_price')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('type_average_price')
        batch_op.drop_column('type_adjusted_price')
