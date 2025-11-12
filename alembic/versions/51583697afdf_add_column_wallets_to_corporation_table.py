"""Add column wallets to corporation table

Revision ID: 51583697afdf
Revises: d9f9e4c80ac7
Create Date: 2025-10-30 11:28:17.036910

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '51583697afdf'
down_revision: Union[str, Sequence[str], None] = 'd9f9e4c80ac7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('corporations') as batch_op:
        batch_op.add_column(sa.Column('wallets', sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('corporations') as batch_op:
        batch_op.drop_column('wallets')
