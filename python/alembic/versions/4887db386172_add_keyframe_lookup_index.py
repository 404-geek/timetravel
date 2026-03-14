"""add keyframe lookup index

Revision ID: 4887db386172
Revises: e56587cdb951
Create Date: 2026-03-12 13:16:15.269096

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4887db386172'
down_revision: Union[str, Sequence[str], None] = 'e56587cdb951'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "idx_record_versions_keyframe_lookup",
        "record_versions",
        ["record_id", "version", "is_keyframe"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("idx_record_versions_keyframe_lookup", table_name="record_versions")
