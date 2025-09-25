import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

# Mac headers
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
})

# # Windows
# session_win = requests.Session()
# session_win.headers.update({
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
#                   'AppleWebKit/537.36 (KHTML, like Gecko) '
#                   'Chrome/116.0.0.0 Safari/537.36'
# })
#
# # Linux
# session_linux = requests.Session()
# session_linux.headers.update({
#     'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '
#                   'AppleWebKit/537.36 (KHTML, like Gecko) '
#                   'Chrome/116.0.0.0 Safari/537.36'
# })

def fetch_country(detail_url) -> str:
    """
    Fetch country/region information from movie detail page
    """
    try:
        # send request to get info
        response = session.get(detail_url, timeout=10)
        response.raise_for_status()  # check

        # parse html
        soup = BeautifulSoup(response.text, 'html.parser')

        # find country info
        country_section = soup.find('li', {'data-testid': 'title-details-origin'})
        if country_section:
            countries = [a.get_text(strip=True) for a in country_section.find_all('a')]
            return ', '.join(countries)

        return 'N/A'

    except Exception as e:
        logging.error(f"Failed to fetch country for {detail_url}: {str(e)}")
        return 'N/A'


def extract_movie_details(movie) -> dict:
    """
    Extract detailed information from a single movie element
    """
    try:
        # Extract movie name
        name_element = movie.find('h3', class_='ipc-title__text')
        name = name_element.get_text().split('. ', 1)[1] if name_element else 'N/A'

        # Extract rating
        rating_element = movie.find('span', class_='ipc-rating-star--rating')
        rating = rating_element.get_text() if rating_element else 'N/A'

        # Extract vote count
        votes_element = movie.find('span', class_='ipc-rating-star--voteCount')
        votes = votes_element.get_text() if votes_element else 'N/A'
        votes = votes.replace('(', '').replace(')', '').replace(',', '').strip() if votes != 'N/A' else 'N/A'

        # Extract metadata (year and so on）
        metadata_items = movie.find_all('span', class_='sc-15ac7568-7 cCsint cli-title-metadata-item')
        year = metadata_items[0].get_text() if len(metadata_items) > 0 else 'N/A'
        genre = metadata_items[2].get_text() if len(metadata_items) > 2 else 'N/A'

        # Country info  be fetched from detail page with web requests
        detail_link = movie.find('a', class_='ipc-title-link-wrapper')
        detail_url = f"https://www.imdb.com{detail_link['href']}" if detail_link and 'href' in detail_link.attrs else None

        # 获取国家/地区信息
        country = fetch_country(detail_url) if detail_url else 'N/A'

        logging.info(f"Processed movie: {name}")
        return {
            'Name': name,
            'Rating': rating,
            'Votes': votes,
            'Year': year,
            'Country': country,
            'Genre': genre
        }

    except Exception as e:
        logging.error(f"Error processing movie: {str(e)}")
        return None


def get_movies_from_html(file_path) -> list:
    """
    Extract all movie information from local HTML file
    """
    try:
        # Read and Parse local HTML file
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, 'html.parser')

        # Get movie list
        movie_list = soup.find_all('li', class_='ipc-metadata-list-summary-item')
        logging.info(f"Found {len(movie_list)} movies in the HTML file")

        # Use multi-threading to process movie information extraction
        movies_data = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(extract_movie_details, movie) for movie in movie_list]

            # Get results
            for future in as_completed(futures):
                result = future.result()
                if result:
                    movies_data.append(result)

        return movies_data

    except Exception as e:
        logging.error(f"Error processing HTML file: {str(e)}")
        return []


if __name__ == "__main__":
    html_file = 'IMDb.html'

    # Get movie data from HTML file
    movies_data = get_movies_from_html(html_file)

    # Save as CSV file
    if movies_data:
        df = pd.DataFrame(movies_data)
        df.to_csv('imdb_movies.csv', index=False)
        logging.info(f"Successfully saved {len(movies_data)} movies to CSV file")
    else:
        logging.warning("No movie data was extracted")
