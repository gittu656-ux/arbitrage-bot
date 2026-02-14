
import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import json

# Add src to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.execution.cloudbet_executor import CloudbetExecutor
from src.execution.polymarket_executor import PolymarketExecutor

class TestInsufficientFunds(unittest.IsolatedAsyncioTestCase):
    
    async def test_cloudbet_insufficient_funds(self):
        print("\n--- Testing Cloudbet Insufficient Funds (MOCKED) ---")
        executor = CloudbetExecutor("fake_key")
        
        # Mock Response
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"message": "Insufficient funds in account"}
        
        executor.client.post = AsyncMock(return_value=mock_response)
        
        # This should trigger the critical log and print
        resp = await executor.place_bet("event_id", "market_url", 2.0, 100.0)
        self.assertIsNone(resp)
        await executor.close()

    async def test_polymarket_insufficient_funds(self):
        print("\n--- Testing Polymarket Insufficient Funds (MOCKED) ---")
        # Mock the entire ClobClient to avoid Web3 initialization
        with patch('src.execution.polymarket_executor.ClobClient') as MockClob:
            instance = MockClob.return_value
            instance.derive_api_key.return_value = {"key": "fake"}
            
            executor = PolymarketExecutor("0x" + "0" * 64)
            
            # Mock failed order response
            instance.post_order.return_value = {
                "success": False,
                "error": "insufficient balance to place order"
            }
            
            # This should trigger the critical log and print
            resp = await executor.place_order("token_id", 0.5, "BUY", 100.0)
            self.assertIsNone(resp)

if __name__ == "__main__":
    unittest.main()
