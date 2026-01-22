import httpx
import uuid
from typing import Dict, Optional
from ..logger import setup_logger

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
                "Content-Type": "application/json"
            },
            timeout=10
        )

    async def place_bet(self, selection_id: str, odds: float, stake: float, currency: str = "USDT") -> Optional[Dict]:
        """
        Place a bet on Cloudbet.
        
        Args:
            selection_id: Outcome/Selection ID
            odds: Expected decimal odds
            stake: Stake amount
            currency: Account currency
        """
        endpoint = "/v1/betting/place-bet"
        url = f"{self.base_url}{endpoint}"
        
        # Cloudbet requires a unique referenceId for idempotency
        payload = {
            "referenceId": str(uuid.uuid4()),
            "currency": currency,
            "stake": stake,
            "odds": odds,
            "selectionId": selection_id
        }
        
        try:
            self.logger.info(f"Placing bet on Cloudbet: {selection_id} @ {odds} for {stake} {currency}")
            
            response = await self.client.post(url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"Cloudbet bet placed successfully: {data.get('betId')}")
                return data
            else:
                self.logger.error(f"Cloudbet bet failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error placing Cloudbet bet: {e}")
            return None

    async def close(self):
        await self.client.aclose()
