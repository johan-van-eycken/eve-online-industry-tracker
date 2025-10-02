"""Add wallets and standings columns to corporations

Revision ID: 620abc091437
Revises: 5e0040959c62
Create Date: 2025-09-08 16:04:36.830658

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '620abc091437'
down_revision: Union[str, Sequence[str], None] = '5e0040959c62'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add wallets and standings columns (use Text or JSON depending on your DB)
    from sqlalchemy import Text
    op.add_column('corporations', sa.Column('wallets', sa.Text(), nullable=True))
    op.add_column('corporations', sa.Column('standings', sa.Text(), nullable=True))
    pass


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('corporations', 'wallets')
    op.drop_column('corporations', 'standings')
    pass
