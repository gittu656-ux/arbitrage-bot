import httpx
import uuid
from typing import Dict, Optional
try:
    from src.logger import setup_logger
except ImportError:
    from logger import setup_logger

class CloudbetExecutor:
    """Handles bet placement on Cloudbet Trading API."""
    
    def __init__(self, api_key: str, base_url: str = "https://sports-api.cloudbet.com"):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.logger = setup_logger("cloudbet_executor")
        self.client = httpx.AsyncClient(
            headers={
                "X-API-Key": self.api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
                # Browser-like headers to bypass Cloudflare WAF
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Origin": "https://www.cloudbet.com",
                "Referer": "https://www.cloudbet.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site"
            },
            timeout=10,
            follow_redirects=True
        )

    async def place_bet(
        self, 
        event_id: str, 
        market_url: str, 
        odds: float, 
        stake: float, 
        currency: str = "USDT"
    ) -> Optional[Dict]:
        """
        Place a bet on Cloudbet V3 API.
        
        Args:
            event_id: Cloudbet Event ID
            market_url: Cloudbet Market URL (e.g. soccer.winner/home)
            odds: Expected decimal odds (price)
            stake: Stake amount
            currency: Account currency
        """
        # Endpoint from official wiki: https://cloudbet.github.io/wiki/en/docs/sports/api/examples/#place-bet-request
        endpoint = "/pub/v3/bets/place"
        url = f"{self.base_url}{endpoint}"
        
        # Cloudbet V3 requires specific payload fields
        payload = {
            "acceptPriceChange": "BETTER",
            "currency": currency,
            "eventId": str(event_id),
            "marketUrl": market_url,
            "price": str(odds), # Cloudbet V3 likes strings for price/stake
            "referenceId": str(uuid.uuid4()),
            "stake": str(stake)
        }
        
        try:
            self.logger.info(f"Placing bet on Cloudbet: {market_url} for Event {event_id} @ {odds} for {stake} {currency}")
            
            response = await self.client.post(url, json=payload)
            
            if response.status_code in (200, 201):
                data = response.json()
                self.logger.info(f"Cloudbet bet placed successfully: {data.get('status')}")
                return data
            else:
                self.logger.error(f"Cloudbet bet failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error placing Cloudbet bet: {e}")
            return None

    async def close(self):
        await self.client.aclose()
