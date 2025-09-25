import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging systemy
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    handlers = [
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)


def extract_movie_details(movie) -> dict:
    """
    Extract detailed information from a single movie element
    """
    try:
        # Extract movie name
        name_element = movie.find('h3', class_ = 'ipc-title__text')
        name = name_element.get_text().split('. ', 1)[1] if name_element else 'N/A'

        # Extract rating
        rating_element = movie.find('span', class_ = 'ipc-rating-star--rating')
        rating = rating_element.get_text() if rating_element else 'N/A'

        # Extract vote count
        votes_element = movie.find('span', class_='ipc-rating-star--voteCount')
        votes = votes_element.get_text() if votes_element else 'N/A'
        votes = votes.replace('(', '').replace(')', '').replace(',', '').strip() if votes != 'N/A' else 'N/A'

        # Extract metadata (year, etc.)
        metadata_items = movie.find_all('span', class_ = 'sc-15ac7568-7 cCsint cli-title-metadata-item')
        year = metadata_items[0].get_text() if len(metadata_items) > 0 else 'N/A'

        # Extract genre from main page (without web requests)
        genre = metadata_items[2].get_text() if len(metadata_items) > 2 else 'N/A'

        # Country info can't be fetched from detail page without web requests
        country = 'N/A'

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
        with open(file_path, 'r', encoding = 'utf-8') as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, 'html.parser')

        # Get movie list
        movie_list = soup.find_all('li', class_='ipc-metadata-list-summary-item')
        logging.info(f"Found {len(movie_list)} movies in the HTML file")

        # Use multi-threading to process movie information extraction
        movies_data = []
        with ThreadPoolExecutor(max_workers = 10) as executor:
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
        df.to_csv('imdb_movies.csv', index = False)
        logging.info(f"Successfully saved {len(movies_data)} movies to CSV file")
        df.to_excel('imdb_movies.xlsx', index = False)
    else:
        logging.warning("No movie data was extracted")
