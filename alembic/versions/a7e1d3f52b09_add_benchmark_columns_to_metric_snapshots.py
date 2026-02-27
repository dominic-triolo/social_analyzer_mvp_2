"""Add benchmark columns to metric_snapshots

Revision ID: a7e1d3f52b09
Revises: 84c3bf335c1d
Create Date: 2026-02-26 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a7e1d3f52b09'
down_revision: Union[str, None] = '84c3bf335c1d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('metric_snapshots', sa.Column('avg_found', sa.Float(), server_default='0.0'))
    op.add_column('metric_snapshots', sa.Column('avg_scored', sa.Float(), server_default='0.0'))
    op.add_column('metric_snapshots', sa.Column('avg_synced', sa.Float(), server_default='0.0'))
    op.add_column('metric_snapshots', sa.Column('funnel_conversion', sa.Float(), server_default='0.0'))
    op.add_column('metric_snapshots', sa.Column('avg_cost_per_lead', sa.Float(), server_default='0.0'))
    op.create_unique_constraint('uq_metric_snapshot_date_platform', 'metric_snapshots', ['date', 'platform'])


def downgrade() -> None:
    op.drop_constraint('uq_metric_snapshot_date_platform', 'metric_snapshots', type_='unique')
    op.drop_column('metric_snapshots', 'avg_cost_per_lead')
    op.drop_column('metric_snapshots', 'funnel_conversion')
    op.drop_column('metric_snapshots', 'avg_synced')
    op.drop_column('metric_snapshots', 'avg_scored')
    op.drop_column('metric_snapshots', 'avg_found')
