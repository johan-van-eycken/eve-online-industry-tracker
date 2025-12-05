"""add blueprint data to character_assets and corporation_assets

Revision ID: 43c860f4864e
Revises: 7ba9e3f5f7e1
Create Date: 2025-11-15 16:59:23.659319

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '43c860f4864e'
down_revision: Union[str, Sequence[str], None] = '7ba9e3f5f7e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('blueprint_runs', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('blueprint_time_efficiency', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('blueprint_material_efficiency', sa.Integer, nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('blueprint_runs', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('blueprint_time_efficiency', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('blueprint_material_efficiency', sa.Integer, nullable=True))
    pass


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('blueprint_runs')
        batch_op.drop_column('blueprint_time_efficiency')
        batch_op.drop_column('blueprint_material_efficiency')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('blueprint_runs')
        batch_op.drop_column('blueprint_time_efficiency')
        batch_op.drop_column('blueprint_material_efficiency')
    pass
