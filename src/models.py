"""
Pydantic models for normalized market data.
"""
from pydantic import BaseModel, Field
from typing import Dict, Optional
from datetime import datetime


class NormalizedMarket(BaseModel):
    """Normalized market schema used across the system."""
    platform: str = Field(..., description="Platform name: 'polymarket' or 'cloudbet'")
    market_id: str = Field(..., description="Unique market identifier")
    title: str = Field(..., description="Market title/question")
    outcomes: Dict[str, float] = Field(..., description="Outcome name -> decimal odds mapping")
    url: str = Field(..., description="Market URL")
    start_time: Optional[datetime] = Field(None, description="Event start time if available")
    metadata: Dict = Field(default_factory=dict, description="Platform-specific metadata (e.g. token IDs)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "platform": "polymarket",
                "market_id": "123",
                "title": "Will Trump win 2024?",
                "outcomes": {"YES": 1.85, "NO": 2.15},
                "url": "https://polymarket.com/event/123",
                "start_time": None
            }
        }


class MatchedMarket(BaseModel):
    """Matched market pair from different platforms."""
    market_name: str
    platform_a: str
    platform_b: str
    market_a: NormalizedMarket
    market_b: NormalizedMarket
    similarity: float = Field(..., ge=0, le=100, description="Similarity score 0-100")


class ArbitrageOpportunity(BaseModel):
    """Detected arbitrage opportunity."""
    market_name: str
    platform_a: str
    platform_b: str
    odds_a: float
    odds_b: float
    outcome_a: Dict
    outcome_b: Dict
    market_a: Dict
    market_b: Dict
    profit_percentage: float
    total_capital: float
    guaranteed_profit: float
    bet_amount_a: float
    bet_amount_b: float

