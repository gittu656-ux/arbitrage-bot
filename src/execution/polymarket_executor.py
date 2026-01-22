import os
import time
from typing import Dict, Optional
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from py_clob_client.clob_types import OrderArgs, ApiCreds
from ..logger import setup_logger

class PolymarketExecutor:
    """Handles order placement on Polymarket CLOB."""
    
    def __init__(self, private_key: str, base_url: str = "https://clob.polymarket.com"):
        self.private_key = private_key
        self.base_url = base_url
        self.logger = setup_logger("polymarket_executor")
        
        # Initialize CLOB client
        # chain_id 137 is Polygon Mainnet
        self.client = ClobClient(
            host=self.base_url,
            key=self.private_key,
            chain_id=POLYGON
        )
        self.creds = None

    def _get_creds(self):
        """Lazy initialization of API credentials."""
        if not self.creds:
            try:
                # This requires signing a message with the private key
                self.creds = self.client.create_api_creds()
                self.client.set_api_creds(self.creds)
                self.logger.info("Polymarket API credentials generated successfully.")
            except Exception as e:
                self.logger.error(f"Failed to generate Polymarket API credentials: {e}")
                raise
        return self.creds

    async def place_order(self, token_id: str, price: float, side: str, amount: float) -> Optional[Dict]:
        """
        Place a limit order on Polymarket.
        
        Args:
            token_id: CLOB token ID for the outcome
            price: Price (0-1)
            side: "BUY" or "SELL"
            amount: Amount in USDC (for BUY) or tokens (for SELL)
        """
        try:
            self._get_creds()
            
            # For arbitrage, we usually want to execute NOW.
            # Using a slightly higher price for BUY to ensure it fills.
            # amount is total USDC to spend
            
            order_args = OrderArgs(
                token_id=token_id,
                price=round(price, 2),
                side=side,
                size=round(amount / price, 2) if side == "BUY" else round(amount, 2),
            )
            
            self.logger.info(f"Placing {side} order on Polymarket: {token_id} @ {price} for {amount} USDC")
            
            # Note: create_order signs and posts the order
            resp = self.client.create_order(order_args)
            
            if resp.get('success'):
                self.logger.info(f"Polymarket order placed successfully: {resp.get('order_id')}")
                return resp
            else:
                self.logger.error(f"Polymarket order failed: {resp.get('errorMsg')}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error placing Polymarket order: {e}")
            return None
