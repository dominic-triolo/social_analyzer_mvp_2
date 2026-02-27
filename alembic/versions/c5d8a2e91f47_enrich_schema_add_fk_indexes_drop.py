"""Enrich schema, add FK indexes, drop metric_snapshots

Revision ID: c5d8a2e91f47
Revises: a7e1d3f52b09
Create Date: 2026-02-27 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c5d8a2e91f47'
down_revision: Union[str, None] = 'a7e1d3f52b09'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # -- Lead: enrichment columns --
    op.add_column('leads', sa.Column('engagement_rate', sa.Float(), nullable=True))
    op.add_column('leads', sa.Column('media_count', sa.Integer(), nullable=True))
    op.add_column('leads', sa.Column('category', sa.Text(), nullable=True))
    op.add_column('leads', sa.Column('location', sa.Text(), nullable=True))
    op.add_column('leads', sa.Column('extra_data', sa.JSON(), nullable=True))

    # -- LeadRun: stage data columns --
    op.add_column('lead_runs', sa.Column('enrichment_data', sa.JSON(), nullable=True))
    op.add_column('lead_runs', sa.Column('content_data', sa.JSON(), nullable=True))
    op.add_column('lead_runs', sa.Column('prescreen_data', sa.JSON(), nullable=True))

    # -- DbRun: errors + stage_timings --
    op.add_column('runs', sa.Column('errors', sa.JSON(), nullable=True))
    op.add_column('runs', sa.Column('stage_timings', sa.JSON(), nullable=True))

    # -- FK indexes on LeadRun --
    op.create_index('ix_lead_runs_lead_id', 'lead_runs', ['lead_id'])
    op.create_index('ix_lead_runs_run_id', 'lead_runs', ['run_id'])

    # -- Drop MetricSnapshot table --
    op.drop_table('metric_snapshots')


def downgrade() -> None:
    # -- Recreate metric_snapshots --
    op.create_table(
        'metric_snapshots',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('platform', sa.Text(), nullable=False),
        sa.Column('yield_rate', sa.Float(), server_default='0.0'),
        sa.Column('avg_score', sa.Float(), server_default='0.0'),
        sa.Column('auto_enroll_rate', sa.Float(), server_default='0.0'),
        sa.Column('tier_distribution', sa.JSON()),
        sa.Column('runs_count', sa.Integer(), server_default='0'),
        sa.Column('avg_found', sa.Float(), server_default='0.0'),
        sa.Column('avg_scored', sa.Float(), server_default='0.0'),
        sa.Column('avg_synced', sa.Float(), server_default='0.0'),
        sa.Column('funnel_conversion', sa.Float(), server_default='0.0'),
        sa.Column('avg_cost_per_lead', sa.Float(), server_default='0.0'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint('date', 'platform', name='uq_metric_snapshot_date_platform'),
    )

    # -- Drop FK indexes --
    op.drop_index('ix_lead_runs_run_id', 'lead_runs')
    op.drop_index('ix_lead_runs_lead_id', 'lead_runs')

    # -- Drop DbRun columns --
    op.drop_column('runs', 'stage_timings')
    op.drop_column('runs', 'errors')

    # -- Drop LeadRun columns --
    op.drop_column('lead_runs', 'prescreen_data')
    op.drop_column('lead_runs', 'content_data')
    op.drop_column('lead_runs', 'enrichment_data')

    # -- Drop Lead columns --
    op.drop_column('leads', 'extra_data')
    op.drop_column('leads', 'location')
    op.drop_column('leads', 'category')
    op.drop_column('leads', 'media_count')
    op.drop_column('leads', 'engagement_rate')
