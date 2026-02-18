import httpx
import uuid
from typing import Dict, Optional
try:
    from src.logger import setup_logger
except ImportError:
    from logger import setup_logger

class CloudbetExecutor:
    """Handles bet placement on Cloudbet Trading API."""
    
    def __init__(self, api_key: str, base_url: str = "https://sports-api.cloudbet.com", proxy: str = None):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.logger = setup_logger("cloudbet_executor")
        
        # Proxy configuration (optional - for bypassing Cloudflare on cloud hosts)
        client_kwargs = {
            "headers": {
                "X-API-Key": self.api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
                # Minimal headers - let proxy handle the rest
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            "timeout": 30,  # Increased timeout for proxy
            "follow_redirects": True
        }
        
        if proxy:
            client_kwargs["proxy"] = proxy  # httpx uses 'proxy' not 'proxies'
            self.logger.info(f"Using proxy for Cloudbet requests: {proxy}")
        
        self.client = httpx.AsyncClient(**client_kwargs)

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
                try:
                    data = response.json()
                    status = data.get('status')
                    if status in ('ACCEPTED', 'PENDING_ACCEPTANCE'):
                        self.logger.info(f"Cloudbet bet placed successfully: {status}")
                        return data
                    else:
                        self.logger.error(f"Cloudbet bet rejected/failed with status: {status}")
                        return None
                except Exception as json_err:
                    self.logger.error(f"Failed to parse success response: {json_err}")
                    return None
            else:
                # Try to get error message safely
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', error_data.get('error', str(error_data)))
                except:
                    # If JSON parsing fails, try text
                    try:
                        error_msg = response.text[:200]  # Limit to first 200 chars
                    except:
                        error_msg = f"Status {response.status_code} (unable to decode response)"
                
                # Highlight insufficient funds
                if "insufficient" in error_msg.lower() or "funds" in error_msg.lower():
                    self.logger.critical(f"INSUFFICIENT FUNDS on Cloudbet! Error: {error_msg}")
                    print("\n[!] [CLOUDBET] INSUFFICIENT FUNDS - Please top up your account.\n")
                else:
                    self.logger.error(f"Cloudbet bet failed: {response.status_code} - {error_msg}")
                
                return None
                
        except Exception as e:
            self.logger.error(f"Error placing Cloudbet bet: {e}", exc_info=True)
            return None

    async def close(self):
        await self.client.aclose()
