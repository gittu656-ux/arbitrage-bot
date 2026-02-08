import inspect
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import os
from dotenv import load_dotenv

load_dotenv()
private_key = os.getenv("POLYMARKET_PRIVATE_KEY")

try:
    client = ClobClient(
        host="https://clob.polymarket.com",
        key=private_key,
        chain_id=POLYGON
    )
    print("Methods available in ClobClient:")
    for name, method in inspect.getmembers(client, predicate=inspect.ismethod):
        if not name.startswith('_'):
            print(f"- {name}")
            
    print("\nAttributes available in ClobClient:")
    for name, value in inspect.getmembers(client):
        if not name.startswith('_') and not inspect.ismethod(value):
             print(f"- {name}")

except Exception as e:
    print(f"Error inspecting client: {e}")
