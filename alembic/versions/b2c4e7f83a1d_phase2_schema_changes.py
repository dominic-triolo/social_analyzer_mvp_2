"""Phase 2 schema changes: summary, cost, stage_outputs, filter_history, presets, metric_snapshots

Revision ID: b2c4e7f83a1d
Revises: 9fde9c88194a
Create Date: 2026-02-25 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b2c4e7f83a1d'
down_revision: Union[str, Sequence[str], None] = '9fde9c88194a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add new columns to runs table
    op.add_column('runs', sa.Column('summary', sa.Text(), nullable=True))
    op.add_column('runs', sa.Column('estimated_cost', sa.Float(), nullable=True))
    op.add_column('runs', sa.Column('actual_cost', sa.Float(), nullable=True))
    op.add_column('runs', sa.Column('stage_outputs', sa.JSON(), nullable=True))

    # Create filter_history table
    op.create_table('filter_history',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('filter_hash', sa.Text(), nullable=False),
        sa.Column('platform', sa.Text(), nullable=False),
        sa.Column('run_id', sa.Text(), nullable=False),
        sa.Column('total_found', sa.Integer(), nullable=True),
        sa.Column('new_found', sa.Integer(), nullable=True),
        sa.Column('novelty_rate', sa.Float(), nullable=True),
        sa.Column('ran_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_filter_history_filter_hash', 'filter_history', ['filter_hash'])

    # Create presets table
    op.create_table('presets',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('platform', sa.Text(), nullable=False),
        sa.Column('filters', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )

    # Create metric_snapshots table
    op.create_table('metric_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('platform', sa.Text(), nullable=False),
        sa.Column('yield_rate', sa.Float(), nullable=True),
        sa.Column('avg_score', sa.Float(), nullable=True),
        sa.Column('auto_enroll_rate', sa.Float(), nullable=True),
        sa.Column('tier_distribution', sa.JSON(), nullable=True),
        sa.Column('runs_count', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('metric_snapshots')
    op.drop_table('presets')
    op.drop_index('ix_filter_history_filter_hash', 'filter_history')
    op.drop_table('filter_history')
    op.drop_column('runs', 'stage_outputs')
    op.drop_column('runs', 'actual_cost')
    op.drop_column('runs', 'estimated_cost')
    op.drop_column('runs', 'summary')
