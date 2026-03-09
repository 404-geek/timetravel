"""Add latest_version to records (Google-Docs style: current = replay, no full-doc overwrite).

Revision ID: add_latest_ver
Revises: add_delta_kf
Create Date: 2026-03-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision = "add_latest_ver"
down_revision: Union[str, Sequence[str], None] = "add_delta_kf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("records", schema=None) as batch_op:
        batch_op.add_column(sa.Column("latest_version", sa.Integer(), nullable=True))
        batch_op.alter_column("data", existing_type=sa.Text(), nullable=True)
    # Backfill: versioned records get latest_version = max(version)
    op.execute(
        """
        UPDATE records SET latest_version = (
            SELECT MAX(version) FROM record_versions WHERE record_versions.record_id = records.id
        ) WHERE EXISTS (SELECT 1 FROM record_versions WHERE record_versions.record_id = records.id)
        """
    )


def downgrade() -> None:
    with op.batch_alter_table("records", schema=None) as batch_op:
        batch_op.alter_column("data", existing_type=sa.Text(), nullable=False)
        batch_op.drop_column("latest_version")
