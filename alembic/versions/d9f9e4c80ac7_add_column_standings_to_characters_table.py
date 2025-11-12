"""Add column standings to characters table

Revision ID: d9f9e4c80ac7
Revises: a8fb81b2f931
Create Date: 2025-10-30 11:26:04.680743

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd9f9e4c80ac7'
down_revision: Union[str, Sequence[str], None] = 'a8fb81b2f931'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('characters') as batch_op:
        batch_op.add_column(sa.Column('standings', sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('characters') as batch_op:
        batch_op.drop_column('standings')
