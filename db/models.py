"""SQLAlchemy models mirroring db/schema.sql.

Kept intentionally thin (Core-style Table objects rather than a full ORM
mapping) since the consumer mostly does simple inserts/updates and doesn't
need relationship loading, lazy loading, etc.
"""
from sqlalchemy import (
    Table,
    Column,
    MetaData,
    String,
    Numeric,
    Float,
    BigInteger,
    DateTime,
    JSON,
    ARRAY,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB

metadata = MetaData()

transactions = Table(
    "transactions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("user_id", String, nullable=False),
    Column("amount", Numeric(12, 2), nullable=False),
    Column("merchant_category", String, nullable=False),
    Column("location", String, nullable=False),
    Column("timestamp", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

user_profiles = Table(
    "user_profiles",
    metadata,
    Column("user_id", String, primary_key=True),
    Column("txn_count", BigInteger, nullable=False, default=0),
    Column("mean_amount", Float, nullable=False, default=0),
    Column("m2_amount", Float, nullable=False, default=0),
    Column("common_categories", JSONB, nullable=False, default=dict),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

flagged_events = Table(
    "flagged_events",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("transaction_id", UUID(as_uuid=True), nullable=False),
    Column("reason", String, nullable=False),
    Column("rule_types", ARRAY(String), nullable=False, default=list),
    Column("score", Float, nullable=False),
    Column("flagged_at", DateTime(timezone=True), nullable=False),
)
