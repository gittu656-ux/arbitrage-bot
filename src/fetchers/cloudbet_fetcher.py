"""
Cloudbet fetcher that traverses the full hierarchy:
Sports → Competitions → Events → Markets → Odds

Fetches ALL available data from Cloudbet API.
"""
import asyncio
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import httpx

try:
    from src.logger import setup_logger
except ImportError:
    from logger import setup_logger


class CloudbetFetcher:
    """Fetches ALL odds from Cloudbet by traversing the full API hierarchy."""
    
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://sports-api.cloudbet.com/pub",
        timeout: int = 10,
        retry_attempts: int = 3,
        retry_delay: int = 2,
        debug_api: bool = False
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.debug_api = debug_api
        self.logger = setup_logger("cloudbet_fetcher")
        
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "X-API-Key": api_key,
                "Accept": "application/json"
            },
            follow_redirects=True
        )
        
        # Statistics for logging
        self.stats = {
            'sports_fetched': 0,
            'competitions_fetched': 0,
            'competitions_with_events': 0,
            'events_fetched': 0,
            'events_with_markets': 0,
            'markets_fetched': 0,
            'outcomes_fetched': 0
        }
    
    async def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request with retry logic."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        if self.debug_api:
            if params:
                from urllib.parse import urlencode
                query_string = urlencode(params)
                full_url = f"{url}?{query_string}"
            else:
                full_url = url
            self.logger.debug(f"Cloudbet request: {full_url}")
        
        for attempt in range(self.retry_attempts):
            try:
                response = await self.client.get(url, params=params)
                
                if self.debug_api:
                    self.logger.debug(f"Response status: {response.status_code}")
                    body_preview = response.text[:500] if response.text else ""
                    self.logger.debug(f"Response preview: {body_preview}")
                
                if response.status_code == 403:
                    raise ValueError(
                        "Cloudbet API key lacks odds permission or environment mismatch. "
                        "Check API key permissions."
                    )
                
                response.raise_for_status()
                return response.json()
                
            except httpx.TimeoutException:
                if attempt < self.retry_attempts - 1:
                    self.logger.warning(f"Timeout on attempt {attempt + 1}/{self.retry_attempts}. Retrying...")
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    self.logger.error("Cloudbet request timeout after all retries")
                    return None
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    raise ValueError(
                        "Cloudbet API key lacks odds permission or environment mismatch."
                    )
                # 400/404 errors are expected for invalid sports/competitions
                if e.response.status_code in (400, 404):
                    return None
                self.logger.error(f"HTTP error {e.response.status_code}: {e}")
                return None
            except Exception as e:
                self.logger.error(f"Error making request: {e}")
                return None
        
        return None
    
    async def get_all_sports(self) -> List[Dict]:
        """
        Step 1: Fetch all available sports.
        GET /v2/odds/sports
        """
        endpoint = "/v2/odds/sports"
        response = await self._make_request(endpoint)
        
        if not response:
            return []
        
        # Extract sports list
        sports = []
        if isinstance(response, dict):
            sports = response.get('sports', response.get('data', []))
        elif isinstance(response, list):
            sports = response
        
        if isinstance(sports, list):
            self.stats['sports_fetched'] = len(sports)
            self.logger.info(f"Fetched {len(sports)} sports from Cloudbet")
            return sports
        
        return []
    
    async def get_competitions_for_sport(self, sport_key: str) -> List[Dict]:
        """
        Step 2: Fetch competitions for a specific sport.
        GET /v2/odds/sports/{sport_key}

        Returns list of competitions extracted from categories.
        """
        endpoint = f"/v2/odds/sports/{sport_key}"
        response = await self._make_request(endpoint)

        if not response:
            return []

        # Extract competitions from categories
        competitions = []
        if isinstance(response, dict):
            # Check for categories structure (sports are organized by categories)
            categories = response.get('categories', [])
            if categories and isinstance(categories, list):
                for category in categories:
                    category_comps = category.get('competitions', [])
                    if category_comps and isinstance(category_comps, list):
                        competitions.extend(category_comps)
            else:
                # Fallback: try direct competitions key
                comps = response.get('competitions', response.get('data', []))
                if isinstance(comps, list):
                    competitions = comps
                elif isinstance(comps, dict):
                    competitions = [comps]
        elif isinstance(response, list):
            competitions = response

        return competitions
    
    async def get_events_for_competition(self, competition_key: str) -> List[Dict]:
        """
        Step 3: Fetch events for a specific competition.
        GET /v2/odds/competitions/{competition_key}
        """
        endpoint = f"/v2/odds/competitions/{competition_key}"
        response = await self._make_request(endpoint)
        
        if not response:
            return []
        
        # Extract events
        events = []
        if isinstance(response, dict):
            events_data = response.get('events', response.get('data', []))
            if isinstance(events_data, list):
                events = events_data
            elif isinstance(events_data, dict):
                # Single event
                events = [events_data]
        elif isinstance(response, list):
            events = response
        
        return events
    
    def _extract_outcomes_from_event(self, event: Dict, sport_key: str, competition_key: str) -> List[Dict]:
        """
        Step 4: Extract all outcomes from an event.
        Processes markets and outcomes within the event.

        Markets structure:
        {
            'market-type-key': {
                'submarkets': {
                    'default': {
                        'selections': [
                            {'outcome': 'name', 'price': 2.5, ...},
                            ...
                        ]
                    }
                }
            }
        }
        """
        outcomes = []

        event_name = event.get('name', 'Unknown Event')
        event_id = event.get('id') or event.get('key', '')
        start_time = event.get('startTime') or event.get('start_time') or event.get('cutoffTime')
        event_status = event.get('status', '')

        # Only process TRADING or TRADING_LIVE events
        if event_status not in ('TRADING', 'TRADING_LIVE'):
            return outcomes

        markets = event.get('markets', {})
        if not markets:
            return outcomes

        # Markets is a dict, not a list
        if not isinstance(markets, dict):
            return outcomes

        self.stats['events_with_markets'] += 1

        # Iterate through market types
        for market_type_key, market_data in markets.items():
            if not isinstance(market_data, dict):
                continue

            self.stats['markets_fetched'] += 1

            # Get submarkets
            submarkets = market_data.get('submarkets', {})
            if not isinstance(submarkets, dict):
                continue

            # Process each submarket (usually 'default')
            for submarket_key, submarket_data in submarkets.items():
                if not isinstance(submarket_data, dict):
                    continue

                # Get selections (outcomes)
                selections = submarket_data.get('selections', [])
                if not isinstance(selections, list):
                    continue

                for selection in selections:
                    # Extract price (odds)
                    price = selection.get('price')
                    if price is None or not isinstance(price, (int, float)):
                        continue

                    # Convert to decimal odds
                    decimal_odds = float(price)
                    if decimal_odds < 1.0:
                        continue

                    # Extract outcome name
                    outcome_slug = selection.get('outcome', selection.get('name', 'Unknown'))
                    
                    # Build marketUrl as required by V3 Trading API
                    # Format: <market_key>/<outcome>?<grouping_parameters>
                    
                    # IMPORTANT: Fix sport-specific market types
                    # Cloudbet sometimes returns invalid combinations (e.g., basketball.1x2)
                    corrected_market_key = market_type_key
                    
                    # Basketball corrections
                    if sport_key == 'basketball-usa-nba' or 'basketball' in sport_key:
                        if '1x2' in market_type_key or 'winner' in market_type_key:
                            corrected_market_key = 'basketball.match_winner'
                        elif 'moneyline' in market_type_key:
                            corrected_market_key = 'basketball.moneyline'
                    
                    # Soccer/Football - 1x2 is valid
                    elif 'soccer' in sport_key or 'football' in sport_key:
                        if 'winner' in market_type_key or '1x2' in market_type_key:
                            corrected_market_key = f"{sport_key.split('-')[0]}.1x2"
                    
                    params = selection.get('params')
                    market_url = f"{corrected_market_key}/{outcome_slug}"
                    if params:
                        market_url += f"?{params}"

                    # Build display URL
                    display_url = f"https://www.cloudbet.com/en/sports/{sport_key}"
                    if competition_key:
                        display_url += f"/{competition_key}"
                    if event_id:
                        display_url += f"/{event_id}"

                    outcomes.append({
                        'platform': 'cloudbet',
                        'event_name': event_name,
                        'market_name': event_name,  # Use event name as market name
                        'market_type': market_type_key,
                        'outcome': outcome_slug,
                        'odds': decimal_odds,
                        'url': display_url,
                        'start_time': start_time,
                        'event_status': event_status,
                        'sport_key': sport_key,
                        'competition_key': competition_key,
                        'selection_id': selection.get('id') or selection.get('key') or outcome_slug,
                        'market_id': event_id,
                        'market_url': market_url,
                        'event_id': event_id
                    })

                    self.stats['outcomes_fetched'] += 1

        return outcomes
    
    async def fetch_all_markets(self) -> List[Dict]:
        """
        Fetch ALL markets by traversing the full hierarchy:
        Sports → Competitions → Events → Markets → Odds
        
        Returns normalized outcomes.
        """
        all_outcomes = []
        
        # Reset statistics
        self.stats = {
            'sports_fetched': 0,
            'competitions_fetched': 0,
            'competitions_with_events': 0,
            'events_fetched': 0,
            'events_with_markets': 0,
            'markets_fetched': 0,
            'outcomes_fetched': 0
        }
        
        # Step 1: Fetch all sports
        self.logger.info("Step 1: Fetching all sports from Cloudbet...")
        sports = await self.get_all_sports()
        
        if not sports:
            self.logger.warning("No sports found - API may be unavailable or empty")
            return []
        
        # Limit to popular sports for faster testing
        # TODO: Remove this filter in production
        popular_sports = ['soccer', 'basketball', 'american-football', 'baseball', 'tennis', 'boxing', 'mma']

        # Step 2: For each sport, fetch competitions
        for sport in sports:
            sport_key = sport.get('key') or sport.get('name') or sport.get('id')
            if not sport_key:
                continue

            # Skip non-popular sports for now
            if sport_key not in popular_sports:
                continue

            sport_name = sport.get('name', sport_key)
            self.logger.debug(f"Processing sport: {sport_name} ({sport_key})")
            
            try:
                competitions = await self.get_competitions_for_sport(sport_key)
                self.stats['competitions_fetched'] += len(competitions)
                
                if not competitions:
                    continue
                
                self.logger.debug(f"  Found {len(competitions)} competitions for {sport_name}")
                
                # Limit number of competitions per sport for testing
                # TODO: Remove this limit in production
                competitions_to_fetch = competitions[:5]  # Only first 5 competitions

                # Step 3: For each competition, fetch events
                for comp in competitions_to_fetch:
                    comp_key = comp.get('key') or comp.get('id')
                    comp_name = comp.get('name', comp_key)

                    if not comp_key:
                        continue

                    try:
                        events = await self.get_events_for_competition(comp_key)
                        self.stats['events_fetched'] += len(events)

                        if not events:
                            continue

                        self.stats['competitions_with_events'] += 1
                        self.logger.debug(f"    Found {len(events)} events in {comp_name}")

                        # Step 4: Extract outcomes from each event
                        for event in events:
                            event_outcomes = self._extract_outcomes_from_event(
                                event, sport_key, comp_key
                            )
                            all_outcomes.extend(event_outcomes)

                        # Rate limiting
                        await asyncio.sleep(0.1)

                    except Exception as e:
                        self.logger.warning(f"Error fetching events for competition {comp_name}: {e}")
                        continue
                
                # Rate limiting between sports
                await asyncio.sleep(0.2)
            
            except Exception as e:
                self.logger.warning(f"Error fetching competitions for sport {sport_name}: {e}")
                continue
        
        # Log statistics
        self.logger.info(
            f"Cloudbet fetch complete: "
            f"{self.stats['sports_fetched']} sports, "
            f"{self.stats['competitions_fetched']} competitions, "
            f"{self.stats['competitions_with_events']} competitions with events, "
            f"{self.stats['events_fetched']} events, "
            f"{self.stats['events_with_markets']} events with markets, "
            f"{self.stats['markets_fetched']} markets, "
            f"{self.stats['outcomes_fetched']} outcomes"
        )
        
        if len(all_outcomes) == 0:
            self.logger.warning("No Cloudbet outcomes found - this is expected if no events are scheduled")
        else:
            self.logger.info(f"Fetched {len(all_outcomes)} total outcomes from Cloudbet")
        
        return all_outcomes
    
    def get_stats(self) -> Dict:
        """Get fetch statistics."""
        return self.stats.copy()
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
