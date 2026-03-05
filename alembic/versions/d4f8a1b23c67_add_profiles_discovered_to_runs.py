"""add profiles_discovered to runs

Revision ID: d4f8a1b23c67
Revises: b3e7f9a12c45
Create Date: 2026-03-05 18:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4f8a1b23c67'
down_revision: Union[str, Sequence[str], None] = 'b3e7f9a12c45'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('runs', sa.Column('profiles_discovered', sa.Integer(), server_default='0'))


def downgrade() -> None:
    op.drop_column('runs', 'profiles_discovered')
