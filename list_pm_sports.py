
import asyncio
import sys
import httpx
from pathlib import Path

async def main():
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get("https://gamma-api.polymarket.com/sports")
        if resp.status_code == 200:
            sports = resp.json()
            print("--- Polymarket Sports List ---")
            for sport in sports:
                name = sport.get('sport')
                series = sport.get('series')
                print(f"Sport: {name}, Series ID: {series}")
        else:
            print(f"Error fetching sports: {resp.status_code}")

if __name__ == '__main__':
    asyncio.run(main())
