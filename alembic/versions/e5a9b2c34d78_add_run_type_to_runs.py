"""add run_type to runs

Revision ID: e5a9b2c34d78
Revises: d4f8a1b23c67, fa0496e6dfeb
Create Date: 2026-03-06 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5a9b2c34d78'
down_revision: Union[str, Sequence[str]] = ('d4f8a1b23c67', 'fa0496e6dfeb')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('runs', sa.Column('run_type', sa.Text(), nullable=False, server_default='discovery'))


def downgrade() -> None:
    op.drop_column('runs', 'run_type')
