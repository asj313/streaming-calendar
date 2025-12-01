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
    page = 1
    
    while page <= 5:  # Max 5 pages to avoid too many requests
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_API_KEY}"
        url += f"&region=US&with_release_type=2|3"  # 2=Limited, 3=Theatrical
        url += f"&release_date.gte={start_date}&release_date.lte={end_date}"
        url += f"&sort_by=release_date.asc&page={page}"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                for movie in data.get('results', []):
                    # Get release date for US
                    release_date = movie.get('release_date', '')
                    if not release_date:
                        continue
                    
                    # Get poster
                    poster_path = movie.get('poster_path')
                    poster_url = f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None
                    
                    # Determine if wide or limited based on popularity/vote count
                    vote_count = movie.get('vote_count', 0)
                    popularity = movie.get('popularity', 0)
                    release_type = "Wide Release" if popularity > 50 or vote_count > 100 else "Limited"
                    
                    releases.append({
                        'title': movie.get('title', ''),
                        'date': release_date,
                        'platform': release_type,
                        'synopsis': movie.get('overview', '')[:200] + '...' if len(movie.get('overview', '')) > 200 else movie.get('overview', ''),
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

def scrape_streaming_month(month: str, year: int) -> list:
    """Scrape streaming releases for a given month."""
    url = get_preview_url(month, year)
    print(f"Fetching streaming: {url}")
    
    try:
        response = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; StreamingCalendar/1.0)'
        })
        response.raise_for_status()
    except Exception as e:
        print(f"Error: {e}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    text = soup.get_text()
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    
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
                    "synopsis": synopsis[:250] + "..." if len(synopsis) > 250 else synopsis,
                    "type": "streaming"
                })
        
        i += 1
    
    return releases

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
    
    # Deduplicate
    seen = set()
    unique = []
    for r in all_releases:
        key = (r['title'].lower(), r['date'])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    
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
