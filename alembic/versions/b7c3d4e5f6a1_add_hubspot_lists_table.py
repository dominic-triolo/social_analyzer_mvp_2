"""add hubspot_lists table

Revision ID: b7c3d4e5f6a1
Revises: e5a9b2c34d78
Create Date: 2026-03-06 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b7c3d4e5f6a1'
down_revision: Union[str, None] = 'e5a9b2c34d78'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'hubspot_lists',
        sa.Column('list_id', sa.Text(), primary_key=True),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('size', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('processing_type', sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table('hubspot_lists')
