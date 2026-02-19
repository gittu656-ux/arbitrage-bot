"""
Event-level matcher for sports events.

Matches events by:
1. Teams (both teams must match)
2. Sport/League
3. Date/Time (within a configurable window)

Then converts all outcomes to probabilities for comparison.
"""
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
from rapidfuzz import fuzz

from .logger import setup_logger
from .sports_matcher import SportsMarketDetector


class EventMatcher:
    """
    Matches sports events at the event level (teams + sport + time).
    
    This is the correct approach for matching Polymarket (prediction markets)
    with Cloudbet (sportsbook), as they use different market structures.
    """
    
    def __init__(
        self,
        team_similarity_threshold: float = 65.0,  # Lowered from 70 to 65 for more matches
        time_window_hours: int = 168  # Increased to 7 days (168 hours) for futures markets
    ):
        """
        Initialize event matcher.
        
        Args:
            team_similarity_threshold: Minimum team name similarity (0-100)
            time_window_hours: Maximum time difference for matching events (hours)
        """
        self.team_similarity_threshold = team_similarity_threshold
        self.time_window_hours = time_window_hours
        self.logger = setup_logger("event_matcher")
        self.detector = SportsMarketDetector()
        self.debug = True  # Enable debug logging
    
    def _normalize_team_name(self, name: str) -> str:
        """Normalize team name for matching."""
        if not name:
            return ""
        
        name = name.lower().strip()
        
        # Remove common prefixes from Cloudbet
        name = name.replace('s-', '').replace('h-', '').replace('a-', '')
        
        # Remove city names and common soccer descriptors
        city_patterns = [
            'los angeles', 'la ', 'new york', 'ny ', 'san francisco', 'sf ',
            'golden state', 'gs ', 'manchester', 'liverpool', 'real', 'fc ', 
            'cf ', 'ac ', 'atlanta', 'boston', 'chicago', 'dallas', 'denver',
            'detroit', 'houston', 'indiana', 'miami', 'milwaukee', 'minnesota',
            'new orleans', 'oklahoma', 'orlando', 'philadelphia', 'phoenix',
            'portland', 'sacramento', 'san antonio', 'toronto', 'utah',
            'washington', 'brooklyn', 'charlotte', 'cleveland'
        ]
        for city in city_patterns:
            name = name.replace(city, '').strip()
        
        # Remove common soccer suffixes/terms
        suffixes = [
            ' fc', ' cf', ' united', ' city', ' town', ' albion', ' athletic', 
            ' county', ' & hove albion', ' rangers', ' hotspur', ' arsenal',
            ' de futbol', ' balompie', ' borussia', ' mnonchengladbach', ' munich'
        ]
        for suffix in suffixes:
            if suffix in name:
                name = name.replace(suffix, '').strip()
        
        # Remove separators
        name = name.replace('-', ' ').replace('_', ' ').replace(',', '').replace('.', '')
        name = ' '.join(name.split())
        
        return name
    
    def _extract_teams(self, title: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract team names from title."""
        return self.detector.extract_teams_from_title(title)
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not dt_str:
            return None
        
        try:
            # Try ISO format
            if 'T' in dt_str or 'Z' in dt_str:
                dt_str = dt_str.replace('Z', '+00:00')
                return datetime.fromisoformat(dt_str)
            
            # Try timestamp (milliseconds or seconds)
            if dt_str.isdigit():
                ts = int(dt_str)
                if ts > 1e10:  # Milliseconds
                    return datetime.utcfromtimestamp(ts / 1000)
                else:  # Seconds
                    return datetime.utcfromtimestamp(ts)
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _teams_match(
        self,
        pm_team1: str,
        pm_team2: str,
        cb_team1: str,
        cb_team2: str
    ) -> Tuple[bool, Dict]:
        """
        Check if two pairs of teams match (order-independent).
        
        Returns (True/False, debug_info) with similarity scores.
        """
        pm1_norm = self._normalize_team_name(pm_team1)
        pm2_norm = self._normalize_team_name(pm_team2)
        cb1_norm = self._normalize_team_name(cb_team1)
        cb2_norm = self._normalize_team_name(cb_team2)
        
        # Try order 1: PM team1 = CB team1, PM team2 = CB team2
        sim1_1 = fuzz.ratio(pm1_norm, cb1_norm)
        sim1_2 = fuzz.ratio(pm2_norm, cb2_norm)
        match1 = (
            sim1_1 >= self.team_similarity_threshold and
            sim1_2 >= self.team_similarity_threshold
        )
        
        # Try order 2: PM team1 = CB team2, PM team2 = CB team1
        sim2_1 = fuzz.ratio(pm1_norm, cb2_norm)
        sim2_2 = fuzz.ratio(pm2_norm, cb1_norm)
        match2 = (
            sim2_1 >= self.team_similarity_threshold and
            sim2_2 >= self.team_similarity_threshold
        )
        
        # Also try token-based matching (more flexible)
        token_sim1_1 = fuzz.token_sort_ratio(pm1_norm, cb1_norm)
        token_sim1_2 = fuzz.token_sort_ratio(pm2_norm, cb2_norm)
        token_match1 = (
            token_sim1_1 >= self.team_similarity_threshold and
            token_sim1_2 >= self.team_similarity_threshold
        )
        
        token_sim2_1 = fuzz.token_sort_ratio(pm1_norm, cb2_norm)
        token_sim2_2 = fuzz.token_sort_ratio(pm2_norm, cb1_norm)
        token_match2 = (
            token_sim2_1 >= self.team_similarity_threshold and
            token_sim2_2 >= self.team_similarity_threshold
        )
        
        result = match1 or match2 or token_match1 or token_match2
        
        debug_info = {
            'pm_teams': (pm_team1, pm_team2),
            'cb_teams': (cb_team1, cb_team2),
            'pm_norm': (pm1_norm, pm2_norm),
            'cb_norm': (cb1_norm, cb2_norm),
            'sim_order1': (sim1_1, sim1_2),
            'sim_order2': (sim2_1, sim2_2),
            'token_sim_order1': (token_sim1_1, token_sim1_2),
            'token_sim_order2': (token_sim2_1, token_sim2_2),
            'match': result
        }
        
        return result, debug_info
    
    def _times_match(
        self,
        pm_time: Optional[datetime],
        cb_time: Optional[datetime]
    ) -> bool:
        """
        Check if two event times are within the allowed window.
        
        If either time is None, assume they match (futures markets don't have times).
        """
        if pm_time is None or cb_time is None:
            return True  # Futures markets or missing time data
        
        time_diff = abs((pm_time - cb_time).total_seconds() / 3600)  # hours
        return time_diff <= self.time_window_hours
    
    def match_events(
        self,
        polymarket_markets: List,
        cloudbet_events: Dict[str, Dict]
    ) -> List[Dict]:
        """
        Match events at the event level (teams + sport + time).
        
        Args:
            polymarket_markets: List of Polymarket NormalizedMarket objects
            cloudbet_events: Dict of Cloudbet events (from _group_cloudbet_by_event)
        
        Returns:
            List of matched events with team and time validation
        """
        matches = []
        
        # Filter Polymarket for sports markets - ONLY GAME MARKETS (two teams), skip futures/props
        sports_markets = []
        futures_count = 0
        for market in polymarket_markets:
            title = market.title if hasattr(market, 'title') else market.get('title', '')
            if self.detector.is_sports_market(title):
                # Extract teams to check if this is a game (two teams) or futures (single team)
                teams = self._extract_teams(title)
                if teams[0] and teams[1]:
                    # This is a game market (two teams) - include it
                    sports_markets.append(market)
                elif teams[0] and not teams[1]:
                    # This is a futures/prop market (single team) - skip it
                    futures_count += 1
                    if self.debug:
                        self.logger.debug(f"Skipping futures market: {title}")
        
        self.logger.info(
            f"Event matching: {len(sports_markets)} Polymarket GAME markets "
            f"(skipped {futures_count} futures/props) vs {len(cloudbet_events)} Cloudbet events"
        )
        
        # Debug: Count how many have extractable teams
        pm_with_teams = 0
        cb_with_teams = 0
        for market in sports_markets[:10]:  # Check first 10
            title = market.title if hasattr(market, 'title') else market.get('title', '')
            teams = self._extract_teams(title)
            if teams[0]:
                pm_with_teams += 1
        
        for event_name in list(cloudbet_events.keys())[:10]:
            teams = self._extract_teams(event_name)
            if teams[0] and teams[1]:
                cb_with_teams += 1
        
        self.logger.info(
            f"Teams extractable: {pm_with_teams}/10 PM markets, {cb_with_teams}/10 CB events"
        )
        
        for pm_market in sports_markets:
            pm_title = pm_market.title if hasattr(pm_market, 'title') else pm_market.get('title', '')
            pm_outcomes = pm_market.outcomes if hasattr(pm_market, 'outcomes') else pm_market.get('outcomes', {})
            pm_dict = pm_market.dict() if hasattr(pm_market, 'dict') else pm_market
            
            # Extract teams from Polymarket title
            pm_teams = self._extract_teams(pm_title)
            
            # Skip if not a game market (must have two teams)
            # We already filtered futures at the top, but double-check here
            if not pm_teams[0] or not pm_teams[1]:
                if self.debug:
                    if not pm_teams[0]:
                        self.logger.debug(f"PM market '{pm_title}' - Could not extract any team")
                    else:
                        self.logger.debug(f"PM market '{pm_title}' - Only one team extracted (futures), skipping")
                continue  # Need both teams for game matching
            
            # Detect sport
            pm_sport = (self.detector.detect_sport(pm_title) or 'unknown')
            
            # Parse Polymarket time if available
            pm_time = None
            if hasattr(pm_market, 'start_time') and pm_market.start_time:
                pm_time = self._parse_datetime(pm_market.start_time)
            elif isinstance(pm_dict, dict) and pm_dict.get('start_time'):
                pm_time = self._parse_datetime(pm_dict['start_time'])
            
            # Try to match with each Cloudbet event
            best_match_score = 0
            best_match = None
            
            for cb_event_key, cb_event_data in cloudbet_events.items():
                cb_event_name = cb_event_data.get('event_name', '')
                cb_sport = cb_event_data.get('sport_key') or 'unknown'
                
                # Sport must match (if both are known) - but be more flexible
                pm_sport_norm = (pm_sport or 'unknown')
                cb_sport_norm = (cb_sport or 'unknown')
                if pm_sport_norm != 'unknown' and cb_sport_norm != 'unknown':
                    # Allow some sport variations (e.g., 'american-football' vs 'nfl')
                    p = pm_sport_norm.replace('-', ' ').lower()
                    c = cb_sport_norm.replace('-', ' ').lower()
                    sport_match = (p == c or p in c or c in p)
                    if not sport_match:
                        continue
                
                # Extract teams from Cloudbet event
                cb_teams = self._extract_teams(cb_event_name)
                
                if not cb_teams[0] or not cb_teams[1]:
                    continue  # Cloudbet events must have both teams (actual games, not props)
                
                # Only match game-to-game (both must have two teams)
                # We already filtered out futures from Polymarket, so this should always be game-to-game
                teams_match, debug_info = self._teams_match(
                    pm_teams[0], pm_teams[1],
                    cb_teams[0], cb_teams[1]
                )
                debug_info['is_futures'] = False
                
                if not teams_match:
                    if self.debug and best_match_score == 0:  # Log first few failures
                        # Handle both single team and two team cases
                        if 'sim_order1' in debug_info:
                            max_sim = max(
                                debug_info['sim_order1'][0], debug_info['sim_order1'][1],
                                debug_info['sim_order2'][0], debug_info['sim_order2'][1]
                            )
                        else:
                            max_sim = debug_info.get('similarity', 0)
                        
                        if max_sim > 50:  # Only log if somewhat close
                            pm_team_str = f"{pm_teams[0]}" + (f" vs {pm_teams[1]}" if pm_teams[1] else "")
                            cb_team_str = f"{cb_teams[0]} vs {cb_teams[1]}" if cb_teams[0] and cb_teams[1] else "N/A"
                            self.logger.debug(
                                f"Teams don't match: PM '{pm_team_str}' "
                                f"vs CB '{cb_team_str}' "
                                f"(max similarity: {max_sim:.1f}%, threshold: {self.team_similarity_threshold}%)"
                            )
                    continue
                
                # Check if times match
                cb_time = self._parse_datetime(cb_event_data.get('start_time'))
                if not self._times_match(pm_time, cb_time):
                    if self.debug:
                        self.logger.debug(f"Times don't match: PM={pm_time}, CB={cb_time}")
                    continue
                
                # Calculate match score (average similarity)
                # Both are game markets (two teams), so use team similarity
                if 'sim_order1' in debug_info:
                    # Two teams matched
                    avg_sim = (
                        max(debug_info['sim_order1'][0], debug_info['sim_order2'][0]) +
                        max(debug_info['sim_order1'][1], debug_info['sim_order2'][1])
                    ) / 2
                else:
                    avg_sim = debug_info.get('similarity', 0)
                
                if avg_sim > best_match_score:
                    best_match_score = avg_sim
                    best_match = {
                        'cb_event_key': cb_event_key,
                        'cb_event_data': cb_event_data,
                        'cb_event_name': cb_event_name,
                        'cb_teams': cb_teams,
                        'cb_time': cb_time,
                        'cb_sport': cb_sport,
                        'debug_info': debug_info,
                        'match_score': avg_sim
                    }
            
            # Use best match if found
            if best_match:
                cb_event_data = best_match['cb_event_data']
                cb_event_name = best_match['cb_event_name']
                cb_teams = best_match['cb_teams']
                cb_time = best_match['cb_time']
                cb_sport = best_match['cb_sport']
                match_score = best_match['match_score']
                
                # Teams and time match - this is a valid event match!
                match = {
                    'market_name': pm_title,
                    'event_name': cb_event_name,
                    'market_a': pm_dict,
                    'market_b': {
                        'platform': 'cloudbet',
                        'event_name': cb_event_name,
                        'outcomes': cb_event_data['outcomes'],
                        'url': cb_event_data.get('url', ''),
                        'start_time': cb_event_data.get('start_time'),
                        'sport_key': cb_sport,
                        'competition_key': cb_event_data.get('competition_key')
                    },
                    'pm_teams': pm_teams,
                    'cb_teams': cb_teams,
                    'pm_time': pm_time.isoformat() if pm_time else None,
                    'cb_time': cb_time.isoformat() if cb_time else None,
                    'sport': pm_sport if pm_sport != 'unknown' else cb_sport,
                    'platform_a': 'polymarket',
                    'platform_b': 'cloudbet',
                    'pm_outcomes': pm_outcomes,
                    'cb_outcomes': cb_event_data['outcomes'],
                    'match_score': match_score,
                    'is_futures_market': False  # Only matching game markets now
                }
                
                matches.append(match)
                
                self.logger.info(
                    f"[MATCH] Event matched: {pm_title} <-> {cb_event_name} "
                    f"(Teams: {pm_teams[0]} vs {pm_teams[1] if pm_teams[1] else 'N/A'}, "
                    f"Sport: {match['sport']}, Score: {match_score:.1f}%)"
                )
        
        self.logger.info(f"Found {len(matches)} event-level matches")
        return matches

