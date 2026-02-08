"""
Sports-specific market matcher for Cloudbet and Polymarket.

This module handles:
1. Sport market identification (filtering Polymarket for sports-related markets)
2. Event-level matching using fuzzy matching on names, dates, and leagues
3. Outcome translation (YES/NO <-> Home/Draw/Away, Team names, etc.)
"""
from typing import List, Dict, Tuple, Optional
from rapidfuzz import fuzz
from datetime import datetime, timedelta
import re

from .logger import setup_logger


class SportsMarketDetector:
    """Detects sports markets in Polymarket using keyword matching."""

    # Sports keywords that indicate a sports market
    SPORTS_KEYWORDS = {
        # Team sports
        'lakers', 'warriors', 'celtics', 'heat', 'bucks', 'nets', 'knicks', 'bulls',
        'yankees', 'dodgers', 'red sox', 'mets', 'astros', 'cubs', 'giants',
        'patriots', 'cowboys', 'packers', 'eagles', 'chiefs', '49ers', 'rams',
        'manchester united', 'liverpool', 'arsenal', 'chelsea', 'barcelona', 'real madrid',
        'bayern munich', 'psg', 'juventus', 'milan', 'inter',

        # Sports terms
        'nba', 'nfl', 'mlb', 'nhl', 'mls', 'premier league', 'la liga', 'serie a', 'soccer', 'futbol',
        'bundesliga', 'champions league', 'world cup', 'super bowl', 'finals',
        'playoff', 'championship', 'match', 'game', 'score', 'win', 'lose',
        'season', 'mvp', 'golden boot', 'touchdown', 'goal', 'home run',
        'vs', 'versus', 'against',

        # Outcomes
        'moneyline', 'spread', 'over/under', 'total', 'handicap',
        'winner', 'champion', 'division', 'conference',

        # Boxing/MMA
        'boxing', 'ufc', 'mma', 'fight', 'bout', 'ko', 'knockout',
        'mayweather', 'mcgregor', 'tyson', 'paul',

        # Tennis
        'tennis', 'grand slam', 'wimbledon', 'us open', 'french open',
        'australian open', 'federer', 'nadal', 'djokovic',

        # Other sports
        'formula 1', 'f1', 'racing', 'olympics', 'gold medal',
        'golf', 'masters', 'pga', 'tiger woods',
    }

    def __init__(self):
        self.logger = setup_logger("sports_detector")

        # Sport-specific keywords for better matching
        self.sport_keywords = {
            'american-football': ['nfl', 'super bowl', 'ravens', 'steelers', 'patriots', 'cowboys', 'chiefs', 'eagles', 'bills', 'bengals', 'browns', 'broncos', 'texans', 'colts', 'jaguars', 'titans', 'jets', 'dolphins', 'chargers', 'raiders', 'packers', 'bears', 'lions', 'vikings', 'saints', 'falcons', 'panthers', 'buccaneers', '49ers', 'seahawks', 'rams', 'cardinals', 'giants', 'commanders'],
            'basketball': ['nba', 'lakers', 'warriors', 'celtics', 'heat', 'bucks', 'nets', 'knicks', 'sixers', 'raptors', 'bulls', 'cavaliers', 'pistons', 'pacers', 'hawks', 'hornets', 'magic', 'wizards', 'nuggets', 'timberwolves', 'thunder', 'trail blazers', 'jazz', 'suns', 'kings', 'clippers', 'mavericks', 'rockets', 'grizzlies', 'pelicans', 'spurs'],
            'ice-hockey': ['nhl', 'stanley cup', 'bruins', 'maple leafs', 'canadiens', 'senators', 'lightning', 'panthers', 'red wings', 'sabres', 'rangers', 'islanders', 'devils', 'flyers', 'penguins', 'capitals', 'blue jackets', 'hurricanes', 'predators', 'blackhawks', 'blues', 'stars', 'wild', 'avalanche', 'flames', 'oilers', 'canucks', 'kraken', 'ducks', 'sharks', 'kings', 'golden knights', 'coyotes'],
            'baseball': ['mlb', 'world series', 'yankees', 'red sox', 'blue jays', 'orioles', 'rays', 'white sox', 'guardians', 'tigers', 'royals', 'twins', 'astros', 'angels', 'athletics', 'mariners', 'rangers', 'braves', 'marlins', 'mets', 'phillies', 'nationals', 'cubs', 'reds', 'brewers', 'pirates', 'cardinals', 'diamondbacks', 'rockies', 'dodgers', 'padres', 'giants'],
            'baseball': ['mlb', 'world series', 'yankees', 'red sox', 'blue jays', 'orioles', 'rays', 'white sox', 'guardians', 'tigers', 'royals', 'twins', 'astros', 'angels', 'athletics', 'mariners', 'rangers', 'braves', 'marlins', 'mets', 'phillies', 'nationals', 'cubs', 'reds', 'brewers', 'pirates', 'cardinals', 'diamondbacks', 'rockies', 'dodgers', 'padres', 'giants'],
            'soccer': [
                'premier league', 'epl', 'fa cup', 'champions league', 'europa', 'la liga', 'bundesliga', 'serie a', 'ligue 1', 'mls', 'world cup', 'euro 20', 'copa america',
                'manchester', 'liverpool', 'chelsea', 'arsenal', 'tottenham', 'leicester', 'everton', 'west ham', 'newcastle', 'aston villa', 'brighton', 'wolves',
                'barcelona', 'real madrid', 'atletico', 'sevilla', 'valencia', 'villarreal',
                'bayern', 'dortmund', 'leipzig', 'leverkusen', 'frankfurt',
                'juventus', 'inter', 'milan', 'napoli', 'roma', 'lazio', 'atalanta',
                'psg', 'monaco', 'lyon', 'marseille', 'lille',
                'ajax', 'benfica', 'porto', 'sporting', 'celtic', 'rangers'
            ],
        }

    def detect_sport(self, title: str) -> str:
        """
        Detect which sport a title belongs to.

        Args:
            title: Market title

        Returns:
            Sport key matching Cloudbet format ('american-football', 'basketball', etc.) or 'unknown'
        """
        title_lower = title.lower()

        for sport, keywords in self.sport_keywords.items():
            for keyword in keywords:
                if keyword in title_lower:
                    return sport

        return 'unknown'

    def is_sports_market(self, title: str) -> bool:
        """
        Determine if a market is sports-related based on title keywords.

        Args:
            title: Market title/question

        Returns:
            True if sports market, False otherwise
        """
        title_lower = title.lower()

        # Check for any sports keywords
        for keyword in self.SPORTS_KEYWORDS:
            if keyword in title_lower:
                return True

        # Check for team vs team pattern (e.g., "Lakers vs Warriors")
        if re.search(r'\b\w+\s+vs\.?\s+\w+\b', title_lower):
            return True

        return False

    def extract_teams_from_title(self, title: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract team names from title using multiple patterns.

        Handles formats like:
        - "Lakers vs Warriors"
        - "Baltimore Ravens - Pittsburgh Steelers"
        - "Will the Lakers beat the Warriors"
        - "ATL Falcons v NO Saints" (abbreviations)
        - "Will the Baltimore Ravens win Super Bowl?" (futures - single team)
        - "Manchester United vs Liverpool - Match Winner"

        Returns:
            Tuple of (team1, team2) or (team, None) for futures, or (None, None) if not found
        """
        # Pattern 1: Team1 vs Team2 or Team1 v Team2 (with abbreviations)
        # Handles: "Nets vs. Wizards: 1H Moneyline", "Lakers vs Warriors", etc.
        match = re.search(r'([A-Za-z\s]+?)\s+v(?:s|\.)?\.?\s+([A-Za-z\s]+?)(?:\s*[-:\(]|\s*$)', title, re.IGNORECASE)
        if match:
            team1 = match.group(1).strip()
            team2 = match.group(2).strip()
            # Clean up trailing words and suffixes like ": 1H Moneyline"
            team1 = re.sub(r'\s+(on|at|in|the|match|winner|game).*$', '', team1, flags=re.IGNORECASE)
            team2 = re.sub(r'\s+(on|at|in|the|match|winner|game).*$', '', team2, flags=re.IGNORECASE)
            # Also remove any trailing colons and what follows
            team2 = re.sub(r'\s*:.*$', '', team2)
            return (team1, team2)

        # Pattern 2: Team1 - Team2 (hyphen separator)
        match = re.search(r'([A-Za-z\s]+?)\s+-\s+([A-Za-z\s]+?)(?:\s*\(|$)', title, re.IGNORECASE)
        if match:
            team1 = match.group(1).strip()
            team2 = match.group(2).strip()
            # Remove common prefixes
            for prefix in ['will the', 'the', 'can']:
                team1 = re.sub(f'^{prefix}\\s+', '', team1, flags=re.IGNORECASE)
                team2 = re.sub(f'^{prefix}\\s+', '', team2, flags=re.IGNORECASE)
            return (team1, team2)

        # Pattern 3: "Will [Team1] beat [Team2]"
        match = re.search(r'will\s+(?:the\s+)?([A-Za-z\s]+?)\s+beat\s+(?:the\s+)?([A-Za-z\s]+?)(?:\s+on|\s+in|\s+at|\?|$)', title, re.IGNORECASE)
        if match:
            team1 = match.group(1).strip()
            team2 = match.group(2).strip()
            return (team1, team2)

        # Pattern 4: "Will [Team1] win against [Team2]"
        match = re.search(r'will\s+(?:the\s+)?([A-Za-z\s]+?)\s+(?:win\s+against|defeat)\s+(?:the\s+)?([A-Za-z\s]+?)(?:\s+on|\s+in|\s+at|\?|$)', title, re.IGNORECASE)
        if match:
            team1 = match.group(1).strip()
            team2 = match.group(2).strip()
            return (team1, team2)
        
        # Pattern 5: "Will [Team] win [Championship]?" (futures - single team)
        # This is for futures markets like "Will the Baltimore Ravens win Super Bowl 2026?"
        match = re.search(r'will\s+(?:the\s+)?([A-Za-z\s]+?)\s+win\s+([A-Za-z\s]+?)(?:\s+\d{4}|\?|$)', title, re.IGNORECASE)
        if match:
            team = match.group(1).strip()
            # Remove "the" if present
            team = re.sub(r'^the\s+', '', team, flags=re.IGNORECASE)
            return (team, None)  # Single team for futures

        # Pattern 6: Abbreviations like "ATL Falcons v NO Saints" or "CIN Bengals v CLE Browns"
        # Match pattern: 2-4 letter code + team name, separated by "v" or "v."
        match = re.search(r'([A-Z]{2,4}\s+[A-Za-z\s]+?)\s+v\.?\s+([A-Z]{2,4}\s+[A-Za-z\s]+?)(?:\s|$)', title)
        if match:
            team1 = match.group(1).strip()
            team2 = match.group(2).strip()
            return (team1, team2)
        
        # Pattern 7: Simple "Team1 v Team2" (any format)
        match = re.search(r'^([A-Za-z\s]+?)\s+v\.?\s+([A-Za-z\s]+?)(?:\s|$)', title)
        if match:
            team1 = match.group(1).strip()
            team2 = match.group(2).strip()
            # Remove common prefixes
            for prefix in ['the']:
                team1 = re.sub(f'^{prefix}\\s+', '', team1, flags=re.IGNORECASE)
                team2 = re.sub(f'^{prefix}\\s+', '', team2, flags=re.IGNORECASE)
            return (team1, team2)
        
        # Pattern 8: "ABBR Team Record" format (e.g., "NYK Knicks 23-12" vs "DET Pistons 26-9")
        # This handles Polymarket's game display format with abbreviations and records
        # Match: 2-4 letter abbreviation + team name + record (optional)
        team_abbr_pattern = r'([A-Z]{2,4})\s+([A-Za-z\s]+?)(?:\s+\d+-\d+)?'
        matches = re.findall(team_abbr_pattern, title)
        if len(matches) >= 2:
            # Extract team names (ignore abbreviations and records)
            team1_abbr, team1_name = matches[0]
            team2_abbr, team2_name = matches[1]
            # Clean team names
            team1_name = team1_name.strip()
            team2_name = team2_name.strip()
            return (team1_name, team2_name)
        
        # Pattern 9: Two team names with records (e.g., "Knicks 23-12" "Pistons 26-9")
        # Match team name followed by record pattern
        team_with_record = r'([A-Za-z\s]+?)\s+\d+-\d+'
        matches = re.findall(team_with_record, title)
        if len(matches) >= 2:
            team1 = matches[0].strip()
            team2 = matches[1].strip()
            # Remove common prefixes
            for prefix in ['the']:
                team1 = re.sub(f'^{prefix}\\s+', '', team1, flags=re.IGNORECASE)
                team2 = re.sub(f'^{prefix}\\s+', '', team2, flags=re.IGNORECASE)
            return (team1, team2)

        return (None, None)


class SportEventMatcher:
    """Matches sports events between Cloudbet and Polymarket."""

    def __init__(self, similarity_threshold: float = 70.0):
        """
        Initialize sports event matcher.

        Args:
            similarity_threshold: Minimum similarity for event matching (0-100)
                Lower than regular market matching since we need more flexibility
        """
        self.similarity_threshold = similarity_threshold
        self.logger = setup_logger("sport_event_matcher")
        self.detector = SportsMarketDetector()

    def _normalize_team_name(self, name: str) -> str:
        """
        Normalize team/player names for better matching.

        Examples:
            "Los Angeles Lakers" -> "lakers"
            "LA Lakers" -> "lakers"
            "s-lakers" -> "lakers" (Cloudbet format)
        """
        name = name.lower().strip()

        # Remove common prefixes from Cloudbet
        name = re.sub(r'^s-', '', name)
        name = re.sub(r'^h-', '', name)  # home
        name = re.sub(r'^a-', '', name)  # away

        # Remove city names for US teams
        name = re.sub(r'\b(los angeles|new york|san francisco|golden state)\b', '', name)
        name = re.sub(r'\b(manchester|liverpool|real|fc|cf|ac)\b', '', name)

        # Remove common separators
        name = name.replace('-', ' ').replace('_', ' ').replace(',', '')

        # Remove extra spaces
        name = ' '.join(name.split())

        return name

    def _calculate_event_similarity(
        self,
        pm_title: str,
        cb_event_name: str,
        cb_start_time: Optional[str] = None
    ) -> float:
        """
        Calculate similarity between Polymarket title and Cloudbet event.

        Args:
            pm_title: Polymarket market title
            cb_event_name: Cloudbet event name
            cb_start_time: Cloudbet event start time (ISO format)

        Returns:
            Similarity score 0-100
        """
        # First try: Extract and match team names directly
        pm_teams = self.detector.extract_teams_from_title(pm_title)
        cb_teams = self.detector.extract_teams_from_title(cb_event_name)

        # If both have teams extracted, compare teams directly
        if pm_teams[0] and cb_teams[0]:
            team1_norm_pm = self._normalize_team_name(pm_teams[0])
            team2_norm_pm = self._normalize_team_name(pm_teams[1] or '')
            team1_norm_cb = self._normalize_team_name(cb_teams[0])
            team2_norm_cb = self._normalize_team_name(cb_teams[1] or '')

            # Try both orderings (team1-team2 and team2-team1)
            # Order 1: PM team1 vs CB team1, PM team2 vs CB team2
            team1_sim_a = fuzz.ratio(team1_norm_pm, team1_norm_cb)
            team2_sim_a = fuzz.ratio(team2_norm_pm, team2_norm_cb)
            avg_sim_a = (team1_sim_a + team2_sim_a) / 2

            # Order 2: PM team1 vs CB team2, PM team2 vs CB team1
            team1_sim_b = fuzz.ratio(team1_norm_pm, team2_norm_cb)
            team2_sim_b = fuzz.ratio(team2_norm_pm, team1_norm_cb)
            avg_sim_b = (team1_sim_b + team2_sim_b) / 2

            # Use better ordering
            team_similarity = max(avg_sim_a, avg_sim_b)

            # If teams match well (>70%), use that as primary signal
            if team_similarity > 70:
                return team_similarity

        # Fallback: Full title comparison
        pm_norm = self._normalize_team_name(pm_title)
        cb_norm = self._normalize_team_name(cb_event_name)

        # Use token sort ratio for flexibility with word order
        similarity = fuzz.token_sort_ratio(pm_norm, cb_norm)

        return similarity

    def find_sports_matches(
        self,
        polymarket_markets: List,
        cloudbet_outcomes: List[Dict],
        platform_a: str = "polymarket",
        platform_b: str = "cloudbet"
    ) -> List[Dict]:
        """
        Find matching sports events between Polymarket and Cloudbet.

        Args:
            polymarket_markets: List of Polymarket NormalizedMarket objects
            cloudbet_outcomes: List of Cloudbet outcome dictionaries
            platform_a: Name of first platform
            platform_b: Name of second platform

        Returns:
            List of matched sport events with outcome mappings
        """
        matches = []

        # Filter Polymarket for sports markets only
        sports_markets = []
        for market in polymarket_markets:
            title = market.title if hasattr(market, 'title') else market.get('title', '')
            if self.detector.is_sports_market(title):
                sports_markets.append(market)

        self.logger.info(
            f"Filtered Polymarket: {len(sports_markets)} sports markets "
            f"out of {len(polymarket_markets)} total"
        )

        if not sports_markets:
            self.logger.warning("No sports markets found in Polymarket")
            return []

        # Group Cloudbet outcomes by event
        cloudbet_events = self._group_cloudbet_by_event(cloudbet_outcomes)

        self.logger.info(f"Grouped Cloudbet into {len(cloudbet_events)} unique events")

        # Match each Polymarket sports market to Cloudbet events
        for pm_market in sports_markets:
            pm_title = pm_market.title if hasattr(pm_market, 'title') else pm_market.get('title', '')
            pm_outcomes = pm_market.outcomes if hasattr(pm_market, 'outcomes') else pm_market.get('outcomes', {})
            pm_dict = pm_market.dict() if hasattr(pm_market, 'dict') else pm_market

            # Detect sport for this Polymarket market
            pm_sport = self.detector.detect_sport(pm_title)

            best_match = None
            best_similarity = 0

            # Try to match with each Cloudbet event
            for cb_event_key, cb_event_data in cloudbet_events.items():
                # Only match events from the same sport
                cb_sport = cb_event_data.get('sport_key', 'unknown')
                if pm_sport != 'unknown' and cb_sport != 'unknown' and pm_sport != cb_sport:
                    continue  # Skip different sports

                similarity = self._calculate_event_similarity(
                    pm_title,
                    cb_event_data['event_name'],
                    cb_event_data.get('start_time')
                )

                if similarity > best_similarity and similarity >= self.similarity_threshold:
                    # Try to map outcomes
                    outcome_mapping = self._map_outcomes(
                        pm_outcomes,
                        cb_event_data['outcomes'],
                        pm_title,
                        cb_event_data['event_name']
                    )

                    if outcome_mapping:
                        best_similarity = similarity
                        best_match = {
                            'market_name': pm_title,
                            'market_a': pm_dict,
                            'market_b': {
                                'platform': platform_b,
                                'event_name': cb_event_data['event_name'],
                                'outcomes': cb_event_data['outcomes'],
                                'url': cb_event_data['url'],
                                'start_time': cb_event_data.get('start_time'),
                                'sport_key': cb_event_data.get('sport_key'),
                                'competition_key': cb_event_data.get('competition_key')
                            },
                            'similarity': similarity,
                            'outcome_mapping': outcome_mapping,
                            'platform_a': platform_a,
                            'platform_b': platform_b
                        }

            if best_match:
                matches.append(best_match)
                self.logger.info(
                    f"Matched: '{pm_title}' <-> '{best_match['market_b']['event_name']}' "
                    f"(similarity: {best_similarity:.1f}%, outcomes: {len(best_match['outcome_mapping'])})"
                )

        self.logger.info(
            f"Found {len(matches)} sports event matches "
            f"(threshold: {self.similarity_threshold}%)"
        )

        return matches

    def _group_cloudbet_by_event(self, outcomes: List[Dict]) -> Dict[str, Dict]:
        """
        Group Cloudbet outcomes by unique event.

        Returns:
            Dict keyed by event_name with aggregated outcome data.
            
        Important:
            - We strongly prefer the *main moneyline* market for each event.
            - Cloudbet exposes many markets per event (spreads, totals,
              alternative lines, etc.) that reuse the same team names with
              different odds.
            - If we naively aggregate all of them, whichever market is processed
              last will "win", which can produce odds that don't actually exist
              in the primary moneyline market (as seen in Telegram screenshots
              like 2.39 when Cloudbet only shows 3.10).
        """
        events = {}

        for outcome in outcomes:
            event_name = outcome.get('event_name', '')

            if event_name not in events:
                events[event_name] = {
                    'event_name': event_name,
                    # Primary outcome mapping we want to use downstream
                    'outcomes': {},
                    # Backup of *all* outcomes so we can gracefully fall back
                    # if an event has no recognised moneyline market.
                    '_all_outcomes': {},
                    'url': outcome.get('url', ''),
                    'start_time': outcome.get('start_time'),
                    'sport_key': outcome.get('sport_key'),
                    'competition_key': outcome.get('competition_key')
                }

            outcome_name = outcome.get('outcome', '')
            odds = outcome.get('odds', 0.0)
            market_type = (outcome.get('market_type') or '').lower().strip()

            if outcome_name and odds > 0:
                # Always track in the backup map
                events[event_name]['_all_outcomes'][outcome_name] = outcome

                # STRICT: Only accept exact moneyline markets
                # Cloudbet's "game lines" is too broad (includes spreads, totals, moneyline)
                # We ONLY want the moneyline market, not spreads or totals
                # 
                # Cloudbet uses format: "sport.market_type" (e.g., "basketball.moneyline")
                # Accept:
                #   - basketball.moneyline, american_football.moneyline
                #   - tennis.winner, mma.winner, boxing.winner
                #   - Generic: moneyline, ml, match-winner, match_winner
                #   - For NON-SOCCER: match_odds (2-way markets)
                # Reject: game lines, spread, total, handicap, etc.
                
                # Normalize market type for comparison
                market_type_lower = market_type.lower()
                
                # Detect if this is a soccer market (3-way risk)
                sport_key = outcome.get('sport_key', '').lower()
                is_soccer = sport_key == 'soccer'
                
                # Check if it's a moneyline/winner market
                is_primary_moneyline = (
                    # Direct moneyline markets (all sports)
                    'moneyline' in market_type_lower or
                    market_type_lower == 'ml' or
                    'match-winner' in market_type_lower or
                    'match_winner' in market_type_lower or
                    # Match odds for NON-SOCCER sports (basketball, hockey, etc.)
                    ('match_odds' in market_type_lower and not is_soccer) or
                    # Soccer: ONLY Draw No Bet to avoid 3-way risk
                    (is_soccer and ('draw_no_bet' in market_type_lower or 'dnb' in market_type_lower)) or
                    # Tennis/MMA/Boxing use "winner"
                    (market_type_lower.endswith('.winner') or market_type_lower == 'winner')
                )
                
                # Explicitly reject non-moneyline markets even if they contain "winner"
                is_rejected = (
                    'game_lines' in market_type_lower or
                    'handicap' in market_type_lower or
                    'asian' in market_type_lower or  # Asian handicap
                    'spread' in market_type_lower or
                    'total' in market_type_lower or
                    'over' in market_type_lower or
                    'under' in market_type_lower or
                    'period' in market_type_lower or  # Exclude period-specific markets
                    'half' in market_type_lower or    # Exclude half-specific markets
                    'quarter' in market_type_lower or # Exclude quarter-specific markets
                    'outright' in market_type_lower or   # Exclude futures/outrights
                    # Reject soccer 3-way markets (1x2, match_odds for soccer)
                    (is_soccer and ('1x2' in market_type_lower or 'match_odds' in market_type_lower))
                )
                
                is_primary_moneyline = is_primary_moneyline and not is_rejected


                if is_primary_moneyline:
                    # Only store if we don't already have this outcome, or if this is a better match
                    existing_data = events[event_name]['outcomes'].get(outcome_name)
                    if existing_data is None:
                        events[event_name]['outcomes'][outcome_name] = {
                            'odds': odds,
                            'event_id': outcome.get('event_id'),
                            'market_url': outcome.get('market_url'),
                            'selection_id': outcome.get('selection_id')
                        }
                        # Log first time we see moneyline for this event
                        if len(events[event_name]['outcomes']) == 1:
                            self.logger.debug(
                                f"Found moneyline market for '{event_name}': "
                                f"market_type='{outcome.get('market_type')}', "
                                f"first outcome: {outcome_name} @ {odds:.2f}"
                            )
                    elif market_type == 'moneyline' or market_type == 'ml':
                        # Overwrite with exact moneyline if we had a different version
                        old_odds = events[event_name]['outcomes'][outcome_name]['odds']
                        events[event_name]['outcomes'][outcome_name] = {
                            'odds': odds,
                            'event_id': outcome.get('event_id'),
                            'market_url': outcome.get('market_url'),
                            'selection_id': outcome.get('selection_id')
                        }
                        if abs(old_odds - odds) > 0.1:  # Log if odds changed significantly
                            self.logger.debug(
                                f"Updated moneyline odds for '{event_name}' {outcome_name}: "
                                f"{old_odds:.2f} -> {odds:.2f} (market_type: {outcome.get('market_type')})"
                            )

        # Post-process: Log which events found moneyline markets and which didn't
        # DO NOT fall back to all outcomes - this causes wrong odds (spreads, totals, etc.)
        # Only use events that have a recognized moneyline market
        events_with_moneyline = 0
        events_without_moneyline = 0
        
        for event_name, data in events.items():
            if data['outcomes']:
                events_with_moneyline += 1
            else:
                events_without_moneyline += 1
                # Log what market types we saw for debugging
                if data['_all_outcomes']:
                    # Sample a few outcomes to see what market types exist
                    sample_outcomes = list(data['_all_outcomes'].keys())[:3]
                    self.logger.debug(
                        f"Event '{event_name}' has no recognized moneyline market. "
                        f"Sample outcomes: {sample_outcomes}. "
                        f"Total outcomes available: {len(data['_all_outcomes'])}"
                    )
                # Remove events without moneyline to prevent wrong odds
                # This is safer than using spreads/totals which have different meanings
            # Remove the internal backup key before returning
            data.pop('_all_outcomes', None)
        
        if events_without_moneyline > 0:
            self.logger.warning(
                f"Filtered out {events_without_moneyline} events without recognized moneyline markets. "
                f"Kept {events_with_moneyline} events with valid moneyline data."
            )

        return events
    
    def _group_cloudbet_by_event_for_matcher(self, outcomes: List[Dict]) -> Dict[str, Dict]:
        """
        Group Cloudbet outcomes by unique event (for EventMatcher compatibility).
        
        This is the same as _group_cloudbet_by_event but kept separate for clarity.
        """
        return self._group_cloudbet_by_event(outcomes)

    def _map_outcomes(
        self,
        pm_outcomes: Dict[str, float],
        cb_outcomes: Dict[str, float],
        pm_title: str,
        cb_event_name: str
    ) -> List[Tuple[Dict, Dict]]:
        """
        Map Polymarket outcomes to Cloudbet outcomes.

        Handles translations like:
        - Polymarket "Yes" -> Cloudbet team name (winner market)
        - Polymarket "No" -> Cloudbet opposite team
        - Direct team name matching

        Args:
            pm_outcomes: Polymarket outcomes {name: odds}
            cb_outcomes: Cloudbet outcomes {name: odds}
            pm_title: Polymarket market title (for context)
            cb_event_name: Cloudbet event name (for context)

        Returns:
            List of (pm_outcome_dict, cb_outcome_dict) tuples
        """
        mappings = []

        # Extract team names from titles
        pm_teams = self.detector.extract_teams_from_title(pm_title)
        cb_teams = self.detector.extract_teams_from_title(cb_event_name)

        pm_outcomes_list = [{'name': k, 'odds': v} for k, v in pm_outcomes.items()]
        # cb_outcomes now contains dicts with 'odds', 'event_id', etc.
        cb_outcomes_list = []
        for name, data in cb_outcomes.items():
            item = data.copy()
            item['name'] = name
            cb_outcomes_list.append(item)

        # Case 1: Polymarket has YES/NO, Cloudbet has team names
        pm_has_yes_no = any(o['name'].upper() in ['YES', 'NO'] for o in pm_outcomes_list)

        if pm_has_yes_no and pm_teams[0] and pm_teams[1]:
            # Try to map YES/NO to teams
            yes_outcome = next((o for o in pm_outcomes_list if o['name'].upper() == 'YES'), None)
            no_outcome = next((o for o in pm_outcomes_list if o['name'].upper() == 'NO'), None)

            # Find which team matches the Polymarket title better
            # e.g., "Will Lakers beat Warriors?" YES = Lakers, NO = Warriors
            for cb_outcome in cb_outcomes_list:
                cb_name_norm = self._normalize_team_name(cb_outcome['name'])

                # Check if this Cloudbet outcome matches team1 or team2
                team1_norm = self._normalize_team_name(pm_teams[0])
                team2_norm = self._normalize_team_name(pm_teams[1])

                team1_sim = fuzz.ratio(cb_name_norm, team1_norm)
                team2_sim = fuzz.ratio(cb_name_norm, team2_norm)

                if team1_sim > 70:
                    # This CB outcome is team1, map to YES (assuming title is "Will team1 win?")
                    if yes_outcome:
                        mappings.append((yes_outcome, cb_outcome))
                elif team2_sim > 70:
                    # This CB outcome is team2, map to NO
                    if no_outcome:
                        mappings.append((no_outcome, cb_outcome))

        # Case 2: Direct team name matching (both have team names)
        else:
            for pm_outcome in pm_outcomes_list:
                pm_name_norm = self._normalize_team_name(pm_outcome['name'])

                best_match = None
                best_sim = 0

                for cb_outcome in cb_outcomes_list:
                    cb_name_norm = self._normalize_team_name(cb_outcome['name'])
                    sim = fuzz.ratio(pm_name_norm, cb_name_norm)

                    if sim > best_sim and sim > 70:
                        best_sim = sim
                        best_match = cb_outcome

                if best_match:
                    mappings.append((pm_outcome, best_match))

        return mappings
