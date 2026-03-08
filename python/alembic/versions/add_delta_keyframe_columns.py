"""Add delta + keyframe columns to record_versions (optional delta storage).

Revision ID: add_delta_kf
Revises: f5c692c18ce2
Create Date: 2026-03-07

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision = "add_delta_kf"
down_revision: Union[str, Sequence[str], None] = "f5c692c18ce2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("record_versions", schema=None) as batch_op:
        batch_op.add_column(sa.Column("delta", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("is_keyframe", sa.Boolean(), nullable=False, server_default=sa.true()))
        batch_op.alter_column(
            "data",
            existing_type=sa.Text(),
            nullable=True,
        )


def downgrade() -> None:
    with op.batch_alter_table("record_versions", schema=None) as batch_op:
        batch_op.alter_column(
            "data",
            existing_type=sa.Text(),
            nullable=False,
        )
        batch_op.drop_column("is_keyframe")
        batch_op.drop_column("delta")
