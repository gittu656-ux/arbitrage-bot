"""
Polymarket fetcher with relaxed filtering.
Logs filtering reasons.
"""
import asyncio
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import httpx

try:
    from src.logger import setup_logger
except ImportError:
    from logger import setup_logger


class PolymarketFetcher:
    """Fetches markets from Polymarket with relaxed filtering."""
    
    def __init__(
        self,
        base_url: str = "https://gamma-api.polymarket.com",
        timeout: int = 10,
        retry_attempts: int = 3,
        retry_delay: int = 2,
        debug_api: bool = False,
        min_liquidity: float = 0.0,  # Relaxed - no minimum
        min_volume: float = 0.0  # Relaxed - no minimum
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.debug_api = debug_api
        self.min_liquidity = min_liquidity
        self.min_volume = min_volume
        self.logger = setup_logger("polymarket_fetcher")
        
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "ArbitrageBot/1.0"
            }
        )
    
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
            self.logger.debug(f"Polymarket request: {full_url}")
        
        for attempt in range(self.retry_attempts):
            try:
                response = await self.client.get(url, params=params)
                
                if self.debug_api:
                    self.logger.debug(f"Response status: {response.status_code}")
                    body_preview = response.text[:500] if response.text else ""
                    self.logger.debug(f"Response preview: {body_preview}")
                
                response.raise_for_status()
                return response.json()
                
            except httpx.TimeoutException:
                if attempt < self.retry_attempts - 1:
                    self.logger.warning(f"Timeout on attempt {attempt + 1}/{self.retry_attempts}. Retrying...")
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    self.logger.error("Polymarket request timeout after all retries")
                    return None
            except Exception as e:
                self.logger.error(f"Error making request: {e}")
                return None
        
        return None
    
    def _convert_price_to_odds(self, price: float) -> Optional[float]:
        """Convert Polymarket price (0-1) to decimal odds."""
        if price <= 0 or price >= 1:
            return None
        return 1.0 / price
    
    def _parse_market(self, market_data: Dict) -> Optional[Dict]:
        """Parse a single market with relaxed filtering."""
        try:
            market_id = market_data.get('id') or market_data.get('conditionId')
            question = (
                market_data.get('question') or
                market_data.get('title') or
                market_data.get('name') or
                market_data.get('description')
            )

            if not question or not market_id:
                return None

            # Get outcomes and prices
            outcomes = {}
            token_ids = {} # Mapping outcome name -> token_id
            
            # Extract CLOB Token IDs if available
            clob_token_ids = market_data.get('clobTokenIds', [])
            if isinstance(clob_token_ids, str):
                try:
                    import json
                    clob_token_ids = json.loads(clob_token_ids)
                except:
                    clob_token_ids = []
            
            # Fallback: check 'tokens' field (sometimes used in event structure)
            if not clob_token_ids:
                tokens = market_data.get('tokens', [])
                if tokens and isinstance(tokens, list):
                    # If tokens is list of dicts, extract token_id/tokenId
                    if len(tokens) > 0 and isinstance(tokens[0], dict):
                        clob_token_ids = [t.get('tokenId', t.get('token_id', '')) for t in tokens]
                    # If list of strings
                    elif len(tokens) > 0 and isinstance(tokens[0], str):
                         clob_token_ids = tokens
            
            # Final Fallback: Query CLOB API directly (Synchronous/Blocking - for reliability)
            if not clob_token_ids:
                condition_id = market_data.get('conditionId')
                if condition_id:
                     try:
                         # self.logger.info(f"Fetching CLOB tokens for {condition_id}...")
                         import httpx
                         resp = httpx.get(f"https://clob.polymarket.com/markets/{condition_id}", timeout=5)
                         if resp.status_code == 200:
                             c_data = resp.json()
                             c_tokens = c_data.get('tokens', [])
                             if c_tokens:
                                 clob_token_ids = [t.get('token_id', '') for t in c_tokens]
                                 # self.logger.info(f"Resolved tokens via CLOB API: {clob_token_ids}")
                     except Exception as e:
                         # self.logger.error(f"CLOB Fallback Failed: {e}")
                         pass

            # Method 1: outcomes list + outcomePrices list (NEW - PRIMARY METHOD)
            outcomes_list = market_data.get('outcomes', [])
            outcome_prices_list = market_data.get('outcomePrices', [])

            # Parse if they're JSON strings
            import json
            if isinstance(outcomes_list, str):
                try:
                    outcomes_list = json.loads(outcomes_list)
                except:
                    outcomes_list = []

            if isinstance(outcome_prices_list, str):
                try:
                    outcome_prices_list = json.loads(outcome_prices_list)
                except:
                    outcome_prices_list = []

            if outcomes_list and outcome_prices_list:
                if isinstance(outcomes_list, list) and isinstance(outcome_prices_list, list):
                    # Map outcomes to prices by index
                    for i, outcome_name in enumerate(outcomes_list):
                        if i < len(outcome_prices_list):
                            price = outcome_prices_list[i]
                            if price is not None:
                                try:
                                    price_float = float(price)
                                    # Skip invalid prices (0, 1, or out of range)
                                    # Valid prices are between 0 and 1 (exclusive)
                                    if price_float > 0 and price_float < 1:
                                        decimal_odds = self._convert_price_to_odds(price_float)
                                        if decimal_odds:
                                            outcomes[outcome_name] = decimal_odds
                                            # Store token_id if available
                                            if i < len(clob_token_ids):
                                                token_ids[outcome_name] = clob_token_ids[i]
                                except (ValueError, TypeError):
                                    continue

            # Method 2: outcomePrices as dict (fallback)
            if not outcomes:
                outcome_prices = market_data.get('outcomePrices', {})
                if outcome_prices and isinstance(outcome_prices, dict):
                    for outcome_name, price in outcome_prices.items():
                        if price is not None:
                            try:
                                price_float = float(price)
                                decimal_odds = self._convert_price_to_odds(price_float)
                                if decimal_odds:
                                    outcomes[outcome_name] = decimal_odds
                            except (ValueError, TypeError):
                                continue

            # Method 3: tokens structure (fallback)
            if not outcomes:
                tokens = market_data.get('tokens', [])
                for token in tokens:
                    outcome_name = (
                        token.get('outcome') or
                        token.get('name') or
                        token.get('side') or
                        token.get('tokenName', '').replace('$', '')
                    )
                    price = token.get('price') or token.get('lastPrice')
                    if price is not None:
                        try:
                            price_float = float(price)
                            decimal_odds = self._convert_price_to_odds(price_float)
                            if decimal_odds:
                                outcomes[outcome_name] = decimal_odds
                        except (ValueError, TypeError):
                            continue

            # Need at least 2 outcomes
            # For game markets, create outcomes from team names even if prices are invalid
            question_lower = question.lower()
            # Check for various "vs" formats: " vs ", " vs. ", "v ", " vs"
            is_game_market = (' vs ' in question_lower or 
                            ' vs. ' in question_lower or 
                            ' v ' in question_lower or
                            ' vs' in question_lower or
                            'v ' in question_lower)
            
            if len(outcomes) < 2:
                # Special case: If this is a game market (has "vs" or team names), 
                # try to create outcomes from team names even without valid prices
                if is_game_market:
                    # This is a game market - extract teams and create synthetic outcomes
                    from ..sports_matcher import SportsMarketDetector
                    detector = SportsMarketDetector()
                    teams = detector.extract_teams_from_title(question)
                    
                    self.logger.debug(f"Game market detected: {question}")
                    self.logger.debug(f"  Extracted teams: {teams}")
                    self.logger.debug(f"  Outcomes list type: {type(outcomes_list)}, value: {outcomes_list}")
                    
                    if teams[0] and teams[1]:
                        # Use team names from outcomes_list if available, otherwise use extracted teams
                        if isinstance(outcomes_list, list) and len(outcomes_list) >= 2:
                            # Use the team names from outcomes_list with default odds
                            # This allows matching even if prices are invalid
                            outcomes = {
                                outcomes_list[0]: 2.0,  # Default odds for matching
                                outcomes_list[1]: 2.0
                            }
                            self.logger.debug(f"Created synthetic outcomes from outcomes_list: {list(outcomes.keys())}")
                        else:
                            # Create synthetic outcomes with extracted team names
                            outcomes = {
                                teams[0]: 2.0,  # Default odds
                                teams[1]: 2.0
                            }
                            self.logger.debug(f"Created synthetic outcomes from extracted teams: {list(outcomes.keys())}")
                        
                        # Don't return None - continue to return the market with synthetic outcomes
                    else:
                        self.logger.debug(f"Could not extract teams from: {question}")
                        return None
                else:
                    # Not a game market and no valid outcomes - skip it
                    return None
            elif is_game_market and len(outcomes) < 2:
                # Even if we have some outcomes, if it's a game market and we don't have 2, try to complete it
                from ..sports_matcher import SportsMarketDetector
                detector = SportsMarketDetector()
                teams = detector.extract_teams_from_title(question)
                if teams[0] and teams[1]:
                    # Add missing team if we have outcomes_list
                    if isinstance(outcomes_list, list):
                        for team_name in outcomes_list:
                            if team_name not in outcomes:
                                outcomes[team_name] = 2.0  # Default odds

            # Create URL
            slug = market_data.get('slug')
            if slug:
                market_url = f"https://polymarket.com/event/{slug}"
            else:
                market_url = f"https://polymarket.com/event/{market_id}"

            return {
                'platform': 'polymarket',
                'market_id': str(market_id),
                'title': question,
                'outcomes': outcomes,
                'url': market_url,
                'start_time': None,  # Polymarket doesn't provide start times
                'metadata': {
                    'token_ids': token_ids,
                    'condition_id': market_data.get('conditionId')
                }
            }

        except Exception as e:
            self.logger.error(f"Error parsing market '{market_data.get('question', 'NO TITLE')}': {e}", exc_info=True)
            return None
    
    async def fetch_all_markets(self, limit: int = 200) -> List[Dict]:
        """
        Fetch markets with relaxed filtering.
        Logs filtering statistics.
        
        Uses Polymarket best practices:
        1. Get sports from /sports endpoint
        2. Get events for specific leagues using series_id
        3. Filter games (not futures) using tag_id=100639
        4. Fetch markets from events
        """
        all_markets = []
        
        # Step 1: Get sports to find NBA/NFL series_id
        sports = await self._make_request("/sports", {})
        
        if sports and isinstance(sports, list):
            self.logger.info(f"Found {len(sports)} sports")
            
            # Target sports and leagues
            TARGET_SPORTS = [
                'nba', 'nfl', 'nhl', 'mlb', 'epl', 'lal', 'bun', 'fl1', 'sea', 'ucl', 'uel', 'mls'
            ]
            target_series_ids = []
            
            for sport in sports:
                sport_name = sport.get('sport', '').lower()
                series = sport.get('series')
                
                if (sport_name in TARGET_SPORTS or any(s in sport_name for s in TARGET_SPORTS)) and series:
                    target_series_ids.append((sport_name, series))
            
            self.logger.info(f"Monitoring series: {[s[0] for s in target_series_ids]}")
            
            # Step 2: Get game events (not futures) using tag_id=100639
            # This filters to just game bets, not futures/props
            GAME_TAG_ID = 100639  # Tag ID for game bets (not futures)
            
            for sport_name, series_id in target_series_ids:
                if not series_id:
                    continue
                
                # Get game events for this league
                events = await self._make_request("/events", {
                    "series_id": series_id,
                    "tag_id": GAME_TAG_ID,  # Filter to games only
                    "active": "true",
                    "closed": "false",
                    "limit": 100
                })
                
                if events and isinstance(events, list):
                    self.logger.info(f"Found {len(events)} game events for series_id {series_id}")
                    
                    # Step 3: Fetch markets from each event
                    # Also create a market from the event title itself (main game market)
                    for event in events[:30]:  # Limit to avoid too many requests
                        event_id = event.get('id')
                        event_title = event.get('title') or event.get('ticker') or ''
                        
                        if event_id:
                            event_details = await self._make_request(f"/events/{event_id}", {})
                            if event_details and isinstance(event_details, dict):
                                markets = event_details.get('markets') or event_details.get('data', [])
                                if isinstance(markets, list):
                                    # Find main game market (moneyline or event title match)
                                    # This is the market we want to match, not props
                                    main_game_market = None
                                    for market in markets:
                                        market_title = market.get('question') or market.get('title') or ''
                                        market_lower = market_title.lower()
                                        
                                        # Check if this is the main game market
                                        # 1. Exact match with event title
                                        # 2. Contains "moneyline" 
                                        # 3. Same as event title (case-insensitive)
                                        # 4. Not a prop (no "over", "under", "points", etc.)
                                        is_prop = any(word in market_lower for word in ['over', 'under', 'points', 'rebounds', 'assists', 'spread'])
                                        
                                        if (market_title.lower() == event_title.lower() or 
                                            ('moneyline' in market_lower and not is_prop) or
                                            (event_title.lower() in market_lower and len(market_title) > 10 and not is_prop)):
                                            main_game_market = market
                                            break
                                    
                                    # Always add event title as market if it has teams (for matching)
                                    # This allows matching even if prices are invalid
                                    # Check for various "vs" formats
                                    has_vs = event_title and (
                                        ' vs ' in event_title.lower() or 
                                        ' vs. ' in event_title.lower() or 
                                        ' v ' in event_title.lower()
                                    )
                                    
                                    if has_vs:
                                        self.logger.debug(f"Processing event: {event_title} (ID: {event_id})")
                                        # Try to get outcomes from main game market first
                                        outcomes = []
                                        outcome_prices = []
                                        
                                        if main_game_market:
                                            outcomes = main_game_market.get('outcomes', [])
                                            outcome_prices = main_game_market.get('outcomePrices', [])
                                        
                                        # If main game market didn't have valid outcomes, try other markets
                                        if not outcomes:
                                            for market in markets:
                                                market_title = market.get('question') or market.get('title') or ''
                                                # Look for any market with team names
                                                if any(team in market_title.lower() for team in event_title.lower().split()):
                                                    outcomes = market.get('outcomes', [])
                                                    outcome_prices = market.get('outcomePrices', [])
                                                    if outcomes:  # Only need outcomes, prices can be invalid
                                                        break
                                        
                                        # Create market from event title (even if prices are invalid)
                                        # The matching will work based on title, prices can be handled later
                                        self.logger.debug(f"  Found outcomes: {outcomes}, outcome_prices: {outcome_prices}")
                                        if outcomes:
                                            # Parse if strings
                                            import json
                                            if isinstance(outcomes, str):
                                                try:
                                                    outcomes = json.loads(outcomes)
                                                except:
                                                    outcomes = []
                                            
                                            if isinstance(outcome_prices, str):
                                                try:
                                                    outcome_prices = json.loads(outcome_prices)
                                                except:
                                                    outcome_prices = []
                                            
                                            # Create market - use event title as the market title
                                            event_market = {
                                                'id': f"event_{event_id}",
                                                'question': event_title,
                                                'title': event_title,
                                                'conditionId': event_id,
                                                'slug': event.get('slug', ''),
                                                'outcomes': outcomes if isinstance(outcomes, list) else [],
                                                'outcomePrices': outcome_prices if isinstance(outcome_prices, list) else [],
                                                'active': event.get('active', True),
                                                'closed': event.get('closed', False),
                                                'startDate': event.get('startDate'),
                                                'endDate': event.get('endDate')
                                            }
                                            all_markets.append(event_market)
                                            self.logger.info(f"Added event market: {event_title} with {len(outcomes)} outcomes")
                                        else:
                                            self.logger.debug(f"  No outcomes found for event: {event_title}")
                                    else:
                                        self.logger.debug(f"  Event title doesn't have 'vs': {event_title}")
                                        
                                        # Also add main game market if found (for actual prices if valid)
                                        if main_game_market and main_game_market not in all_markets:
                                            all_markets.append(main_game_market)
                                    
                                    # Don't add all markets - only add props if we didn't find main game market
                                    # This prevents props from overwhelming the game markets
                                    if not main_game_market and event_title and (' vs ' in event_title.lower() or ' v ' in event_title.lower()):
                                        # Only add a few props, not all
                                        # Add markets that might have valid prices
                                        for market in markets[:5]:  # Limit props
                                            all_markets.append(market)
                                    elif main_game_market or (event_title and (' vs ' in event_title.lower() or ' v ' in event_title.lower())):
                                        # We have a game market, skip props
                                        pass
                                    else:
                                        # No game market found, add some markets
                                        all_markets.extend(markets[:10])
                                    
                                    if len(all_markets) >= limit * 2:
                                        break
                        
                        # Rate limiting
                        await asyncio.sleep(0.1)
                    
                    if len(all_markets) >= limit * 2:
                        break
        
        # Fallback to /markets endpoint if events didn't work or didn't return enough
        self.logger.info(f"Total markets from events: {len(all_markets)}, limit: {limit}")
        if len(all_markets) < limit:
            self.logger.info(f"Falling back to /markets endpoint (need {limit}, have {len(all_markets)})")
            endpoint = "/markets"
            params = {
                "closed": "false",  # Only get non-closed markets
                "limit": limit * 2  # Fetch more to account for filtering
            }
            
            try:
                response = await self._make_request(endpoint, params=params)
                
                if response:
                    # Polymarket returns a list directly
                    markets_data = response
                    if isinstance(response, dict):
                        markets_data = response.get('data', response.get('markets', []))
                    
                    if isinstance(markets_data, list):
                        all_markets.extend(markets_data)
            except Exception as e:
                self.logger.warning(f"Error fetching from /markets endpoint: {e}")
        
        # Use all_markets (from events + /markets)
        markets_data = all_markets
        
        if not markets_data:
            self.logger.warning("No markets found from any endpoint")
            return []
        
        if not isinstance(markets_data, list):
            self.logger.warning(f"Unexpected response format: {type(markets_data)}")
            return []
        
        try:
            
            # Parse with relaxed filtering
            parsed_markets = []
            filter_stats = {
                'closed': 0,
                'archived': 0,
                'inactive': 0,
                'expired': 0,
                'low_liquidity': 0,
                'low_volume': 0,
                'insufficient_outcomes': 0
            }
            
            for market_data in markets_data:
                # For markets from events, they can have closed=True but active=True
                # Use active status as primary filter, not closed
                active = market_data.get('active')
                closed = market_data.get('closed')
                archived = market_data.get('archived')
                
                # Skip if explicitly archived
                if archived == True:
                    filter_stats['archived'] += 1
                    continue
                
                # Skip if not active (active=False or missing and closed=True)
                # Markets from events can be closed=True but active=True (game in progress)
                if active == False or (active is None and closed == True):
                    filter_stats['closed'] += 1
                    continue

                # Check endDate - only skip if clearly expired
                # For game markets, endDate might be game end time, but market might still be active
                # Only filter if market is also not active
                end_date = market_data.get('endDate') or market_data.get('endDateIso')
                if end_date and active != True:  # Only check expiry if not active
                    try:
                        # Parse ISO format
                        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                        # Get current time with timezone
                        now = datetime.now(end_dt.tzinfo)
                        # Add buffer - markets might expire slightly after endDate
                        if end_dt < now - timedelta(hours=24):  # Only filter if expired more than 24 hours ago
                            filter_stats['expired'] += 1
                            continue
                    except (ValueError, AttributeError):
                        pass  # If we can't parse, include it

                # Parse market (don't filter on liquidity/volume - accept all)
                parsed = self._parse_market(market_data)
                if parsed:
                    parsed_markets.append(parsed)
                    if len(parsed_markets) >= limit:
                        break
                else:
                    filter_stats['insufficient_outcomes'] += 1
            
            # Log filtering statistics
            total = len(markets_data)
            self.logger.info(
                f"Polymarket filtering: {len(parsed_markets)}/{total} markets passed. "
                f"Filtered: closed={filter_stats['closed']}, archived={filter_stats['archived']}, "
                f"inactive={filter_stats['inactive']}, expired={filter_stats['expired']}, "
                f"low_liquidity={filter_stats['low_liquidity']}, low_volume={filter_stats['low_volume']}, "
                f"insufficient_outcomes={filter_stats['insufficient_outcomes']}"
            )
            
            return parsed_markets
            
        except Exception as e:
            self.logger.error(f"Error fetching Polymarket markets: {e}", exc_info=True)
            return []
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()

