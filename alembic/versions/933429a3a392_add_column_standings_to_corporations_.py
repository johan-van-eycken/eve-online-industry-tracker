"""Add column standings to corporations table

Revision ID: 933429a3a392
Revises: 51583697afdf
Create Date: 2025-10-30 11:29:31.827943

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '933429a3a392'
down_revision: Union[str, Sequence[str], None] = '51583697afdf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('corporations') as batch_op:
        batch_op.add_column(sa.Column('standings', sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('corporations') as batch_op:
        batch_op.drop_column('standings')
