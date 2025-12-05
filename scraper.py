#!/usr/bin/env python3
"""
Streaming Calendar Scraper
Fetches streaming release data from whentostream.com
Fetches Letterboxd ratings for each movie
Automatically handles current + next month
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime
from pathlib import Path

MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]

# TMDB API for posters
TMDB_API_KEY = "3f9482f67e4249d66b4df84f2fa62c99"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w154"

PLATFORM_PATTERNS = {
    "Netflix": r"\(Netflix\)",
    "Prime Video": r"\(Prime Video\)",
    "HBO Max": r"\(HBO Max\)|Max\)",
    "Hulu": r"\(Hulu\)",
    "Disney+": r"\(Disney\+\)",
    "Paramount+": r"\(Paramount\+\)",
    "Apple TV": r"\(Apple TV\)",
    "Peacock": r"\(Peacock\)",
    "Shudder": r"\(Shudder\)",
    "Starz": r"\(Starz\)",
    "MUBI": r"\(MUBI\)",
    "VOD/Digital": r"\(VOD/Digital\)|\(PVOD\)",
    "MGM+": r"\(MGM\+\)",
    "Criterion": r"\(Criterion\)",
    "Tubi": r"\(Tubi\)",
}

def get_preview_url(month: str, year: int) -> str:
    return f"https://whentostream.com/when-to-streams-{month}-{year}-preview/"

def get_calendar_url(month: str, year: int) -> str:
    return f"https://whentostream.com/streaming-{month}-{year}/"

def title_to_letterboxd_slug(title: str) -> str:
    """Convert movie title to Letterboxd URL slug."""
    # Remove year if present at end
    title = re.sub(r'\s*\(\d{4}\)\s*$', '', title)
    # Convert to lowercase, replace spaces/special chars with hyphens
    slug = title.lower()
    slug = re.sub(r"[:'\"!?,.]", '', slug)  # Remove punctuation
    slug = re.sub(r'[–—]', '-', slug)  # Normalize dashes
    slug = re.sub(r'\s+', '-', slug)  # Spaces to hyphens
    slug = re.sub(r'-+', '-', slug)  # Multiple hyphens to single
    slug = slug.strip('-')
    return slug

def get_tmdb_theatrical_releases(start_date: str, end_date: str) -> list:
    """Fetch theatrical releases from TMDB for a date range."""
    releases = []
    
    # Get the year from start_date to filter out re-releases
    target_year = int(start_date[:4])
    
    # Sort by popularity to get the notable releases first
    url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_API_KEY}"
    url += f"&region=US&with_release_type=2|3"  # 2=Limited, 3=Theatrical
    url += f"&release_date.gte={start_date}&release_date.lte={end_date}"
    url += f"&sort_by=popularity.desc"
    
    page = 1
    while page <= 3:  # Get top 60 most popular
        page_url = url + f"&page={page}"
        
        try:
            response = requests.get(page_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                for movie in data.get('results', []):
                    release_date = movie.get('release_date', '')
                    if not release_date:
                        continue
                    
                    # Skip any movie not from current/next year
                    movie_year = int(release_date[:4]) if release_date else 0
                    if movie_year < 2025:
                        print(f"  Skipping old movie: {movie.get('title')} ({movie_year})")
                        continue
                    
                    # Skip re-releases: high vote count means it's an old classic
                    vote_count = movie.get('vote_count', 0)
                    if vote_count > 1000:
                        print(f"  Skipping likely re-release: {movie.get('title')} (votes: {vote_count})")
                        continue
                    
                    # Get poster
                    poster_path = movie.get('poster_path')
                    poster_url = f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None
                    
                    # Better wide vs limited detection based on popularity
                    popularity = movie.get('popularity', 0)
                    # Wide releases typically have popularity > 10 before release
                    # Major blockbusters have 30+
                    if popularity > 25:
                        release_type = "Wide Release"
                    elif popularity > 8:
                        release_type = "Wide Release"  # Moderate wide releases
                    else:
                        release_type = "Limited"
                    
                    releases.append({
                        'title': movie.get('title', ''),
                        'date': release_date,
                        'platform': release_type,
                        'synopsis': movie.get('overview', ''),
                        'type': 'theatrical',
                        'poster': poster_url,
                        'tmdb_id': movie.get('id'),
                        'letterboxd_rating': None,
                        'letterboxd_url': None
                    })
                
                if page >= data.get('total_pages', 1):
                    break
                page += 1
            else:
                break
        except Exception as e:
            print(f"Error fetching TMDB page {page}: {e}")
            break
        
        time.sleep(0.25)  # Rate limiting
    
    return releases


def get_tmdb_poster(title: str, year: str = None) -> str:
    """Fetch poster URL from TMDB."""
    try:
        # Search for the movie
        search_url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_API_KEY}&query={requests.utils.quote(title)}"
        if year:
            search_url += f"&year={year}"
        
        response = requests.get(search_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('results') and len(data['results']) > 0:
                poster_path = data['results'][0].get('poster_path')
                if poster_path:
                    return f"{TMDB_IMAGE_BASE}{poster_path}"
    except Exception as e:
        pass
    
    return None

def get_letterboxd_rating(title: str, year: str = None) -> dict:
    """Fetch Letterboxd rating and poster for a movie."""
    slug = title_to_letterboxd_slug(title)
    
    # Try with year suffix first if provided
    urls_to_try = [f"https://letterboxd.com/film/{slug}/"]
    if year:
        urls_to_try.insert(0, f"https://letterboxd.com/film/{slug}-{year}/")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    for url in urls_to_try:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                result = {'url': url, 'rating': None, 'poster': None}
                
                # Get rating from meta tags
                rating_meta = soup.find('meta', {'name': 'twitter:data2'})
                if rating_meta:
                    rating_text = rating_meta.get('content', '')
                    match = re.search(r'([\d.]+)\s*out of', rating_text)
                    if match:
                        result['rating'] = float(match.group(1))
                
                # Get poster from og:image meta tag
                poster_meta = soup.find('meta', {'property': 'og:image'})
                if poster_meta:
                    poster_url = poster_meta.get('content', '')
                    if poster_url and 'letterboxd' in poster_url:
                        result['poster'] = poster_url
                
                # Alternative: look for poster in the page
                if not result['poster']:
                    poster_img = soup.find('img', class_='image')
                    if poster_img:
                        result['poster'] = poster_img.get('src', '')
                
                if result['rating'] or result['poster']:
                    return result
                        
        except Exception as e:
            pass
        
        time.sleep(0.3)  # Be nice to their servers
    
    return None

def extract_platform(text: str) -> str:
    for platform, pattern in PLATFORM_PATTERNS.items():
        if re.search(pattern, text, re.IGNORECASE):
            return platform
    return "Unknown"

def parse_date_header(text: str) -> str:
    """Parse 'Monday, December 1st, 2025' to '2025-12-01'"""
    clean = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', text)
    try:
        dt = datetime.strptime(clean.strip(), "%A, %B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except:
        return None

def scrape_movie_page(url: str) -> dict:
    """Scrape individual movie page for details."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; StreamingCalendar/1.0)'}
        response = requests.get(url, timeout=30, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        
        info = {'url': url}
        distributor = None
        
        for line in lines:
            # Look for SVOD release date with platform (e.g., "SVOD Release Date: January 9, 2026 (Netflix)")
            if 'SVOD Release Date:' in line and 'date' not in info:
                match = re.search(r'SVOD Release Date:\s*(\w+ \d+, \d+)\s*\(([^)]+)\)', line)
                if match:
                    date_str = match.group(1)
                    platform = match.group(2)
                    try:
                        dt = datetime.strptime(date_str, "%B %d, %Y")
                        info['date'] = dt.strftime("%Y-%m-%d")
                        info['platform'] = platform
                    except:
                        pass
            
            # Also check VOD Release Date (e.g., "VOD Release Date: December 9, 2025")
            if 'VOD Release Date:' in line and 'date' not in info:
                match = re.search(r'VOD Release Date:\s*(\w+ \d+, \d+)', line)
                if match:
                    date_str = match.group(1)
                    try:
                        dt = datetime.strptime(date_str, "%B %d, %Y")
                        info['date'] = dt.strftime("%Y-%m-%d")
                        info['platform'] = 'VOD/Digital'
                    except:
                        pass
            
            # Capture distributor
            if 'Distributor' in line:
                if 'MUBI' in line:
                    distributor = 'MUBI'
                elif 'Netflix' in line:
                    distributor = 'Netflix'
                elif 'Hulu' in line:
                    distributor = 'Hulu'
                elif 'Amazon' in line or 'Prime' in line:
                    distributor = 'Prime Video'
                elif 'HBO' in line or 'Max' in line:
                    distributor = 'HBO Max'
            
            if 'Synopsis:' in line:
                info['synopsis'] = line.replace('Synopsis:', '').strip()
        
        # Use distributor as platform if we only have VOD/Digital
        if info.get('platform') == 'VOD/Digital' and distributor:
            info['platform'] = distributor
        
        return info
    except Exception as e:
        print(f"    Error fetching {url}: {e}")
        return None

def scrape_calendar_page(month: str, year: int) -> list:
    """Scrape the calendar-style page for movie links, then fetch each movie page."""
    url = get_calendar_url(month, year)
    print(f"Fetching calendar page: {url}")
    
    # Build target month prefix for filtering (e.g., "2025-12")
    month_num = MONTHS.index(month.lower()) + 1
    target_prefix = f"{year}-{month_num:02d}"
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; StreamingCalendar/1.0)'}
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"  Failed: {e}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find movie page links (they have year in URL but aren't calendar pages)
    movie_urls = []
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        if f'-{year}/' in href and 'streaming-' not in href and 'theaters-' not in href and 'whentostream.com' in href:
            if href not in movie_urls:
                movie_urls.append(href)
    
    print(f"  Found {len(movie_urls)} movie links")
    
    releases = []
    for movie_url in movie_urls:
        # Extract title from URL
        title = movie_url.split('/')[-2]
        title = re.sub(r'-\d{4}$', '', title)  # Remove year
        title = title.replace('-', ' ').title()
        
        print(f"    Fetching: {title}")
        movie_info = scrape_movie_page(movie_url)
        
        if movie_info and movie_info.get('date') and movie_info.get('platform'):
            # Only include if the date is in the target month
            if movie_info['date'].startswith(target_prefix):
                releases.append({
                    'title': title,
                    'date': movie_info['date'],
                    'platform': movie_info['platform'],
                    'synopsis': movie_info.get('synopsis', ''),
                    'type': 'streaming'
                })
            else:
                print(f"      Skipping: date {movie_info['date']} not in {target_prefix}")
        
        time.sleep(0.3)  # Rate limiting
    
    return releases

def scrape_streaming_month(month: str, year: int) -> list:
    """Scrape streaming releases for a given month."""
    all_releases = []
    
    # Try preview URL first for the main list
    url = get_preview_url(month, year)
    print(f"Fetching streaming preview: {url}")
    
    response = None
    try:
        response = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; StreamingCalendar/1.0)'
        })
        response.raise_for_status()
        
        # Check if we got actual content (not just homepage)
        if 'Synopsis:' not in response.text:
            print(f"  Preview page has no movie data")
            response = None
    except Exception as e:
        print(f"  Preview failed: {e}")
        response = None
    
    # Parse preview page if we got it
    if response:
        print(f"  Success! Parsing preview page...")
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        lines = [l.strip() for l in text.split('\n') if l.strip()]
    else:
        # No preview, just use calendar
        print(f"  No preview available, using calendar page only...")
        return scrape_calendar_page(month, year)
    
    releases = []
    current_date = None
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check for date header
        if re.match(r'^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+\w+\s+\d+', line):
            parsed = parse_date_header(line)
            if parsed:
                current_date = parsed
            i += 1
            continue
        
        # Check for movie title with platform
        platform_match = None
        for platform, pattern in PLATFORM_PATTERNS.items():
            if re.search(pattern, line, re.IGNORECASE):
                platform_match = platform
                break
        
        if platform_match and current_date:
            title_match = re.match(r'^(.+?)\s*\([^)]+\)\s*$', line)
            if title_match:
                title = title_match.group(1).strip()
                title = re.sub(r'^\[|\]$', '', title)
                
                if len(title) < 2 or title.lower() in ['synopsis', 'cast']:
                    i += 1
                    continue
                
                # Look for synopsis
                synopsis = ""
                for j in range(i + 1, min(i + 5, len(lines))):
                    if lines[j].startswith("Synopsis:"):
                        synopsis = lines[j].replace("Synopsis:", "").strip()
                        break
                    if re.match(r'^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),', lines[j]):
                        break
                
                releases.append({
                    "title": title.title() if title.isupper() else title,
                    "date": current_date,
                    "platform": platform_match,
                    "synopsis": synopsis,
                    "type": "streaming"
                })
        
        i += 1
    
    all_releases.extend(releases)
    
    # ALSO check the calendar page for any new additions not in the preview
    print(f"  Also checking calendar page for updates...")
    calendar_releases = scrape_calendar_page(month, year)
    all_releases.extend(calendar_releases)
    
    return all_releases

def get_months_to_scrape():
    """Get current month and next month."""
    now = datetime.now()
    months = []
    
    for offset in [0, 1]:
        month_idx = (now.month - 1 + offset) % 12
        year = now.year + ((now.month + offset - 1) // 12)
        months.append((MONTHS[month_idx], year))
    
    return months

def main():
    output_dir = Path(__file__).parent / "data"
    output_dir.mkdir(exist_ok=True)
    
    all_releases = []
    months = get_months_to_scrape()
    
    for month_name, year in months:
        releases = scrape_streaming_month(month_name, year)
        all_releases.extend(releases)
        print(f"  Found {len(releases)} streaming releases for {month_name.title()} {year}")
    
    # Deduplicate - prefer specific platforms over VOD/Digital
    seen = {}
    for r in all_releases:
        key = r['title'].lower()
        if key not in seen:
            seen[key] = r
        else:
            # If existing is VOD/Digital but new one is a specific platform, prefer the new one
            if seen[key]['platform'] == 'VOD/Digital' and r['platform'] != 'VOD/Digital':
                seen[key] = r
            # If both are specific platforms, prefer the one with the later date (streaming date)
            elif seen[key]['platform'] != 'VOD/Digital' and r['platform'] != 'VOD/Digital':
                if r['date'] > seen[key]['date']:
                    seen[key] = r
    
    unique = list(seen.values())
    unique.sort(key=lambda x: x['date'])
    
    # Fetch Letterboxd ratings and TMDB posters for each movie
    print("\nFetching Letterboxd ratings and TMDB posters...")
    for i, release in enumerate(unique):
        # Extract year from date
        year = release['date'][:4] if release.get('date') else None
        
        # Get Letterboxd rating
        rating_info = get_letterboxd_rating(release['title'], year)
        if rating_info:
            release['letterboxd_rating'] = rating_info.get('rating')
            release['letterboxd_url'] = rating_info.get('url')
        else:
            release['letterboxd_rating'] = None
            release['letterboxd_url'] = None
        
        # Get TMDB poster
        poster_url = get_tmdb_poster(release['title'], year)
        release['poster'] = poster_url
        
        rating_str = str(rating_info.get('rating')) if rating_info and rating_info.get('rating') else 'no rating'
        poster_str = '✓ poster' if poster_url else 'no poster'
        print(f"  {release['title']}: {rating_str}, {poster_str}")
        
        # Progress indicator
        if (i + 1) % 10 == 0:
            print(f"  [{i + 1}/{len(unique)} complete]")
    
    data = {
        "last_updated": datetime.now().isoformat(),
        "months": [{"name": m.title(), "year": y} for m, y in months],
        "releases": unique,
        "theatrical": []
    }
    
    # Fetch theatrical releases from TMDB
    print("\nFetching theatrical releases from TMDB...")
    theatrical_releases = []
    for month_name, year in months:
        month_num = MONTHS.index(month_name.lower()) + 1
        # Get first and last day of month
        start_date = f"{year}-{month_num:02d}-01"
        if month_num == 12:
            end_date = f"{year}-12-31"
        else:
            end_date = f"{year}-{month_num:02d}-28"  # Safe end date
        
        month_releases = get_tmdb_theatrical_releases(start_date, end_date)
        theatrical_releases.extend(month_releases)
        print(f"  Found {len(month_releases)} theatrical releases for {month_name.title()} {year}")
    
    # Deduplicate theatrical
    seen_theatrical = set()
    unique_theatrical = []
    for r in theatrical_releases:
        key = (r['title'].lower(), r['date'])
        if key not in seen_theatrical:
            seen_theatrical.add(key)
            unique_theatrical.append(r)
    
    unique_theatrical.sort(key=lambda x: x['date'])
    
    # Fetch Letterboxd ratings for theatrical releases
    print("\nFetching Letterboxd ratings for theatrical releases...")
    for i, release in enumerate(unique_theatrical):
        year = release['date'][:4] if release.get('date') else None
        rating_info = get_letterboxd_rating(release['title'], year)
        
        if rating_info:
            release['letterboxd_rating'] = rating_info.get('rating')
            release['letterboxd_url'] = rating_info.get('url')
        
        if (i + 1) % 10 == 0:
            print(f"  [{i + 1}/{len(unique_theatrical)} complete]")
    
    data["theatrical"] = unique_theatrical
    print(f"  Total theatrical: {len(unique_theatrical)}")
    
    output_file = output_dir / "releases.json"
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\nSaved {len(unique)} total releases to {output_file}")

if __name__ == "__main__":
    main()
