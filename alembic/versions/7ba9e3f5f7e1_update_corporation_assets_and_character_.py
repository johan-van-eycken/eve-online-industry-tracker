"""update corporation_assets and character_assets with type faction and type race data aswell type_meta_group_id

Revision ID: 7ba9e3f5f7e1
Revises: 780aa77d1d8f
Create Date: 2025-11-13 21:20:17.331700

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7ba9e3f5f7e1'
down_revision: Union[str, Sequence[str], None] = '780aa77d1d8f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.add_column(sa.Column('type_meta_group_id', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('type_race_id', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('type_race_name', sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column('type_race_description', sa.Text, nullable=True))
        batch_op.add_column(sa.Column('type_faction_id', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('type_faction_name', sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column('type_faction_description', sa.Text, nullable=True))
        batch_op.add_column(sa.Column('type_faction_short_description', sa.Text, nullable=True))
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.add_column(sa.Column('type_meta_group_id', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('type_race_id', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('type_race_name', sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column('type_race_description', sa.Text, nullable=True))
        batch_op.add_column(sa.Column('type_faction_id', sa.Integer, nullable=True))
        batch_op.add_column(sa.Column('type_faction_name', sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column('type_faction_description', sa.Text, nullable=True))
        batch_op.add_column(sa.Column('type_faction_short_description', sa.Text, nullable=True))

def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('character_assets') as batch_op:
        batch_op.drop_column('type_meta_group_id')
        batch_op.drop_column('type_race_id')
        batch_op.drop_column('type_race_name')
        batch_op.drop_column('type_race_description')
        batch_op.drop_column('type_faction_id')
        batch_op.drop_column('type_faction_name')
        batch_op.drop_column('type_faction_description')
        batch_op.drop_column('type_faction_short_description')
    with op.batch_alter_table('corporation_assets') as batch_op:
        batch_op.drop_column('type_meta_group_id')
        batch_op.drop_column('type_race_id')
        batch_op.drop_column('type_race_name')
        batch_op.drop_column('type_race_description')
        batch_op.drop_column('type_faction_id')
        batch_op.drop_column('type_faction_name')
        batch_op.drop_column('type_faction_description')
        batch_op.drop_column('type_faction_short_description')