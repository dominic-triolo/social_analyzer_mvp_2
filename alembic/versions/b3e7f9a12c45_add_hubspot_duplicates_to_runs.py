"""add hubspot_duplicates to runs

Revision ID: b3e7f9a12c45
Revises: fa0496e6dfeb
Create Date: 2026-03-05 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3e7f9a12c45'
down_revision: Union[str, Sequence[str], None] = 'c3d5f8a91b2e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('runs', sa.Column('hubspot_duplicates', sa.Integer(), server_default='0'))


def downgrade() -> None:
    op.drop_column('runs', 'hubspot_duplicates')
