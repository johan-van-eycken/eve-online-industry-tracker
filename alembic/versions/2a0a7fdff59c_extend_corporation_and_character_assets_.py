"""extend corporation and character assets table

Revision ID: 2a0a7fdff59c
Revises: 7ba9e3f5f7e1
Create Date: 2025-12-04 14:14:58.586683

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2a0a7fdff59c'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('is_container', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('is_asset_safety_wrap', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('is_ship', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('is_office_folder', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('top_location_id', sa.BigInteger, nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('is_container', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('is_asset_safety_wrap', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('is_ship', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('is_office_folder', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('top_location_id', sa.BigInteger, nullable=True))

def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('is_container')
        batch_op.drop_column('is_asset_safety_wrap')
        batch_op.drop_column('is_ship')
        batch_op.drop_column('is_office_folder')
        batch_op.drop_column('top_location_id')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('is_container')
        batch_op.drop_column('is_asset_safety_wrap')
        batch_op.drop_column('is_ship')
        batch_op.drop_column('is_office_folder')
        batch_op.drop_column('top_location_id')
