"""Pydantic response models for the API — FastAPI uses these to validate
and document responses (auto-generated docs at /docs)."""
from datetime import datetime
from typing import List
from pydantic import BaseModel


class FlaggedEventOut(BaseModel):
    id: str
    transaction_id: str
    user_id: str
    amount: float
    merchant_category: str
    reason: str
    rule_types: List[str]
    score: float
    flagged_at: datetime


class SummaryStatsOut(BaseModel):
    total_transactions: int
    total_flagged: int
    flag_rate: float  # 0.0 - 1.0
    avg_flagged_score: float


class ReasonCount(BaseModel):
    rule_type: str
    count: int


class TimeseriesPoint(BaseModel):
    bucket_start: datetime
    flagged_count: int
    total_count: int
