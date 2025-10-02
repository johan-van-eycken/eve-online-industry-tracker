"""add is_corp_director column to characters

Revision ID: 5e0040959c62
Revises: 
Create Date: 2025-09-02 01:59:57.131758

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5e0040959c62'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add column is_corp_director, default False
    op.add_column('characters', sa.Column('is_corp_director', sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade() -> None:
    # Remove column if downgrading
    op.drop_column('characters', 'is_corp_director')
