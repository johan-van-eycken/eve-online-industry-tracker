"""Add type_capacity to character_assets and corporation_assets

Revision ID: 8c1541ca18c0
Revises: b57060ab86dd
Create Date: 2025-11-12 15:58:44.242733

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8c1541ca18c0'
down_revision: Union[str, Sequence[str], None] = 'b57060ab86dd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('type_capacity', sa.Float, nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('type_capacity', sa.Float, nullable=True))

def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('type_capacity')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('type_capacity')
