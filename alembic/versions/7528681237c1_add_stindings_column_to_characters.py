"""Add stindings column to characters

Revision ID: 7528681237c1
Revises: 620abc091437
Create Date: 2025-09-08 16:14:10.106667

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7528681237c1'
down_revision: Union[str, Sequence[str], None] = '620abc091437'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    from sqlalchemy import Text
    op.add_column('characters', sa.Column('standings', sa.Text(), nullable=True))
    pass


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('characters', 'standings')
    pass
