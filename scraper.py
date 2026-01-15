"""
Event Calendar Scraper with Advanced Filtering
Runs automatically via GitHub Actions
"""

import requests
from bs4 import BeautifulSoup
from icalendar import Calendar, Event
from datetime import datetime
from dateutil import parser
import json
import re
from typing import List, Dict, Optional
from abc import ABC, abstractmethod
from pathlib import Path


class EventSource(ABC):
    @abstractmethod
    def fetch_events(self) -> List[Dict]:
        pass


class GenericHTMLSource(EventSource):
    def __init__(self, url: str, selectors: Dict):
        self.url = url
        self.selectors = selectors

    def fetch_events(self) -> List[Dict]:
        events = []
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; EventBot/1.0)'}
            response = requests.get(self.url, timeout=15, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            event_elements = soup.select(self.selectors.get('container', '.event'))

            for elem in event_elements:
                try:
                    event = {
                        'title': self._extract_text(elem, self.selectors.get('title')),
                        'start': self._extract_date(elem, self.selectors.get('date')),
                        'location': self._extract_text(elem, self.selectors.get('location')),
                        'description': self._extract_text(elem, self.selectors.get('description')),
                        'url': self._extract_url(elem, self.selectors.get('url'))
                    }
                    if event['title'] and event['start']:
                        events.append(event)
                except Exception as e:
                    print(f"  ⚠️  Error parsing event: {e}")
                    continue

        except Exception as e:
            print(f"  ❌ Error fetching {self.url}: {e}")

        return events

    def _extract_text(self, elem, selector):
        if not selector:
            return ""
        found = elem.select_one(selector)
        return found.text.strip() if found else ""

    def _extract_date(self, elem, selector):
        text = self._extract_text(elem, selector)
        if not text:
            return None
        try:
            return parser.parse(text, fuzzy=True)
        except:
            return None

    def _extract_url(self, elem, selector):
        if not selector:
            return ""
        found = elem.select_one(selector)
        if found and found.get('href'):
            href = found['href']
            if href.startswith('/'):
                from urllib.parse import urljoin
                return urljoin(self.url, href)
            return href
        return ""


class SchemaOrgSource(EventSource):
    def __init__(self, url: str):
        self.url = url

    def fetch_events(self) -> List[Dict]:
        events = []
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; EventBot/1.0)'}
            response = requests.get(self.url, timeout=15, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    events_data = []

                    if isinstance(data, list):
                        events_data = [item for item in data if item.get('@type') == 'Event']
                    elif data.get('@type') == 'Event':
                        events_data = [data]

                    for event_data in events_data:
                        events.append(self._parse_event(event_data))

                except Exception as e:
                    continue

        except Exception as e:
            print(f"  ❌ Error fetching {self.url}: {e}")

        return events

    def _parse_event(self, data):
        start_date = None
        end_date = None

        if data.get('startDate'):
            try:
                start_date = parser.parse(data['startDate'])
            except:
                pass

        if data.get('endDate'):
            try:
                end_date = parser.parse(data['endDate'])
            except:
                pass

        return {
            'title': data.get('name', ''),
            'start': start_date,
            'end': end_date,
            'location': self._extract_location(data.get('location', {})),
            'description': data.get('description', ''),
            'url': data.get('url', '')
        }

    def _extract_location(self, loc):
        if isinstance(loc, dict):
            if loc.get('name'):
                return loc['name']
            if isinstance(loc.get('address'), dict):
                addr = loc['address']
                parts = [addr.get('streetAddress', ''), addr.get('addressLocality', '')]
                return ', '.join(p for p in parts if p)
            return str(loc.get('address', ''))
        return str(loc) if loc else ''


class EventFilter:
    """Filter events based on various criteria"""

    def __init__(self, filter_config: Dict):
        self.config = filter_config

    def filter_events(self, events: List[Dict]) -> List[Dict]:
        """Apply all enabled filters"""
        filtered = events

        # Location filters
        if self.config.get('locations', {}).get('enabled'):
            filtered = self._filter_by_location(filtered)

        # Keyword filters
        if self.config.get('keywords', {}).get('enabled'):
            filtered = self._filter_by_keywords(filtered)

        # Date range filters
        if self.config.get('date_range', {}).get('enabled'):
            filtered = self._filter_by_date_range(filtered)

        return filtered

    def _filter_by_location(self, events: List[Dict]) -> List[Dict]:
        """Filter events by location"""
        loc_config = self.config.get('locations', {})
        include = loc_config.get('include', [])
        exclude = loc_config.get('exclude', [])

        filtered = []
        for event in events:
            location = event.get('location', '').lower()

            # Check exclusions first
            if any(ex.lower() in location for ex in exclude):
                continue

            # If include list exists, event must match at least one
            if include:
                if any(inc.lower() in location for inc in include):
                    filtered.append(event)
            else:
                # No include list, just check it passed exclusions
                filtered.append(event)

        return filtered

    def _filter_by_keywords(self, events: List[Dict]) -> List[Dict]:
        """Filter events by keywords in title/description"""
        kw_config = self.config.get('keywords', {})
        include = kw_config.get('include', [])
        exclude = kw_config.get('exclude', [])

        filtered = []
        for event in events:
            text = f"{event.get('title', '')} {event.get('description', '')}".lower()

            # Check exclusions
            if any(ex.lower() in text for ex in exclude):
                continue

            # Check inclusions
            if include:
                if any(inc.lower() in text for inc in include):
                    filtered.append(event)
            else:
                filtered.append(event)

        return filtered

    def _filter_by_date_range(self, events: List[Dict]) -> List[Dict]:
        """Filter events by date range"""
        dr_config = self.config.get('date_range', {})

        start_date = None
        end_date = None

        if dr_config.get('start_date'):
            try:
                start_date = parser.parse(dr_config['start_date'])
            except:
                pass

        if dr_config.get('end_date'):
            try:
                end_date = parser.parse(dr_config['end_date'])
            except:
                pass

        filtered = []
        for event in events:
            event_date = event.get('start')
            if not event_date:
                continue

            if start_date and event_date < start_date:
                continue

            if end_date and event_date > end_date:
                continue

            filtered.append(event)

        return filtered

    def get_location_groups(self, events: List[Dict]) -> Dict[str, List[Dict]]:
        """Group events by location for separate calendars"""
        groups = {}

        for event in events:
            location = event.get('location', 'Unknown')

            # Normalize location name for grouping
            location_key = self._normalize_location(location)

            if location_key not in groups:
                groups[location_key] = []

            groups[location_key].append(event)

        return groups

    def _normalize_location(self, location: str) -> str:
        """Normalize location string for grouping"""
        # Extract city name (basic heuristic)
        location = location.strip()

        # Common patterns
        if ',' in location:
            # "123 Street, London" -> "London"
            parts = [p.strip() for p in location.split(',')]
            return parts[-1] if parts else location

        # Return as-is if can't parse
        return location


class CalendarGenerator:
    def __init__(self, calendar_name: str = "Aggregated Events"):
        self.calendar_name = calendar_name

    def generate(self, events: List[Dict], calendar_name: Optional[str] = None) -> str:
        cal = Calendar()
        cal.add('prodid', '-//Event Aggregator//EN')
        cal.add('version', '2.0')
        cal.add('x-wr-calname', calendar_name or self.calendar_name)
        cal.add('x-wr-timezone', 'UTC')

        for event_data in events:
            if not event_data.get('start'):
                continue

            event = Event()
            event.add('summary', event_data.get('title', 'Untitled Event'))
            event.add('dtstart', event_data['start'])

            if event_data.get('end'):
                event.add('dtend', event_data['end'])

            if event_data.get('location'):
                event.add('location', event_data['location'])

            if event_data.get('description'):
                event.add('description', event_data['description'])

            if event_data.get('url'):
                event.add('url', event_data['url'])

            uid = f"{hash(event_data.get('title', '') + str(event_data.get('start', '')))}@event-aggregator"
            event.add('uid', uid)
            event.add('dtstamp', datetime.now())

            cal.add_component(event)

        return cal.to_ical().decode('utf-8')


class EventAggregator:
    def __init__(self, sources: List[EventSource], config: Dict):
        self.sources = sources
        self.config = config
        self.event_filter = EventFilter(config.get('filters', {}))
        self.generator = CalendarGenerator(config.get('calendar_name', 'My Events'))

    def collect_events(self) -> List[Dict]:
        all_events = []
        for i, source in enumerate(self.sources, 1):
            print(f"\n📥 Source {i}/{len(self.sources)}: {source.__class__.__name__}")
            events = source.fetch_events()
            print(f"   ✓ Found {len(events)} events")
            all_events.extend(events)
        return all_events

    def generate_calendars(self):
        print("🗓️  Event Calendar Aggregator with Filtering")
        print("=" * 60)

        # Collect all events
        all_events = self.collect_events()
        print(f"\n📊 Total events collected: {len(all_events)}")

        # Apply filters
        filtered_events = self.event_filter.filter_events(all_events)
        print(f"✅ Events after filtering: {len(filtered_events)}")

        if len(all_events) != len(filtered_events):
            print(f"   🔍 Filtered out {len(all_events) - len(filtered_events)} events")

        outputs = self.config.get('outputs', {})

        # Generate main calendar
        main_file = outputs.get('main_calendar', 'events.ics')
        self._save_calendar(filtered_events, main_file)
        print(f"\n✅ Main calendar: {main_file} ({len(filtered_events)} events)")

        # Generate location-specific calendars
        if outputs.get('by_location', False):
            location_groups = self.event_filter.get_location_groups(filtered_events)
            print(f"\n📍 Generating {len(location_groups)} location-specific calendars:")

            for location, events in location_groups.items():
                # Create safe filename
                safe_location = re.sub(r'[^\w\s-]', '', location).strip().replace(' ', '_')
                filename = f"events_{safe_location}.ics"
                self._save_calendar(events, filename, calendar_name=f"Events - {location}")
                print(f"   ✓ {filename} ({len(events)} events)")

        # Generate monthly calendars
        if outputs.get('by_month', False):
            month_groups = self._group_by_month(filtered_events)
            print(f"\n📅 Generating {len(month_groups)} monthly calendars:")

            for month_key, events in month_groups.items():
                filename = f"events_{month_key}.ics"
                self._save_calendar(events, filename, calendar_name=f"Events - {month_key}")
                print(f"   ✓ {filename} ({len(events)} events)")

        print("\n✨ Done!")

    def _save_calendar(self, events: List[Dict], filename: str, calendar_name: Optional[str] = None):
        """Save events to ICS file"""
        ics_content = self.generator.generate(events, calendar_name)
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(ics_content)

    def _group_by_month(self, events: List[Dict]) -> Dict[str, List[Dict]]:
        """Group events by month"""
        groups = {}
        for event in events:
            if event.get('start'):
                month_key = event['start'].strftime('%Y-%m')
                if month_key not in groups:
                    groups[month_key] = []
                groups[month_key].append(event)
        return groups


def load_config(config_file: str = 'config.json') -> Dict:
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️  Config file '{config_file}' not found, using defaults")
        return {
            "calendar_name": "My Events",
            "filters": {},
            "outputs": {"main_calendar": "events.ics"},
            "sources": []
        }


def create_sources_from_config(config: Dict) -> List[EventSource]:
    """Create event sources from configuration"""
    sources = []

    for source_config in config.get('sources', []):
        if not source_config.get('enabled', True):
            continue

        source_type = source_config.get('type', 'schema')
        url = source_config.get('url')

        if not url:
            continue

        try:
            if source_type == 'html':
                selectors = source_config.get('selectors', {})
                sources.append(GenericHTMLSource(url, selectors))
            elif source_type == 'schema':
                sources.append(SchemaOrgSource(url))
        except Exception as e:
            print(f"⚠️  Error creating source for {url}: {e}")

    return sources


def main():
    # Load configuration
    config = load_config()

    # Create sources from config
    sources = create_sources_from_config(config)

    if not sources:
        print("❌ No sources configured. Please edit config.json")
        return

    # Create aggregator and generate calendars
    aggregator = EventAggregator(sources=sources, config=config)
    aggregator.generate_calendars()


if __name__ == "__main__":
    main()