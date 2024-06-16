import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from tqdm.asyncio import tqdm
import re
import os
import logging
import csv
from aiohttp import ClientTimeout, ClientError

# Set up logging
logging.basicConfig(filename='download_books.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Define User-Agent header for HTTP requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

# Asynchronously fetch text content from a URL with retries and timeout handling
async def fetch_text(session, url):
    retries = 3
    timeout = ClientTimeout(total=60)  # Set a total request timeout of 60 seconds
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    try:
                        return await response.text()
                    except UnicodeDecodeError:
                        # Attempt alternative encoding if UTF-8 decode fails
                        raw_text = await response.read()
                        return raw_text.decode('ISO-8859-1')
                else:
                    logging.error(f"Failed to fetch {url}: Status code {response.status}")
                    continue  # Proceed to the next attempt
        except asyncio.TimeoutError:
            logging.warning(f"Timeout occurred when fetching {url}")
        except Exception as e:
            logging.error(f"Error fetching {url}: {str(e)}")
        if attempt < retries - 1:
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    return None

# Attempt to download book data from various file types and versions
async def get_book_data(session, book_id):
    base_url = f"https://www.gutenberg.org/files/{book_id}"
    # List of possible file extensions and versions to try
    extensions = ["epub", "html", "pdf", "txt", "txt.utf8"]
    for i in range(10):  # Assume: Check versions from 0 to 9
        for ext in extensions:
            url = f"{base_url}/{book_id}-{i}.{ext}"
            if ext in ["epub", "html"]:  # For EPUB and HTML, also check "no images" variants
                url_no_images = f"{base_url}/{book_id}-{i}.{ext}.noimages"
                text = await fetch_text(session, url_no_images)
                if text:
                    logging.info(f"Found valid text at {url_no_images}")
                    return text
            text = await fetch_text(session, url)
            if text:
                logging.info(f"Found valid text at {url}")
                return text
            else:
                logging.info(f"No valid text at {url}")
    
    # Also check for files without version numbers
    for ext in extensions:
        url = f"{base_url}/{book_id}.{ext}"
        if ext in ["epub", "html"]:  # For EPUB and HTML, also check "no images" variants
            url_no_images = f"{base_url}/{book_id}.{ext}.noimages"
            text = await fetch_text(session, url_no_images)
            if text:
                logging.info(f"Found valid text at {url_no_images}")
                return text
        text = await fetch_text(session, url)
        if text:
            logging.info(f"Found valid text at {url}")
            return text
        else:
            logging.info(f"No valid text at {url}")

    logging.warning(f"Book ID {book_id}: All URL patterns failed.")
    return None

# Fetch metadata such as year and language for a given book ID
async def get_book_metadata(session, book_id):
    url = f"https://www.gutenberg.org/ebooks/{book_id}"
    metadata = {'year': "Unknown", 'language': "Unknown"}
    text = await fetch_text(session, url)
    if text:
        soup = BeautifulSoup(text, 'html.parser')
        metadata_table = soup.find('table', class_='bibrec')
        if metadata_table:
            for row in metadata_table.find_all('tr'):
                th_text = row.find('th').get_text() if row.find('th') else ''
                td_text = row.find('td').get_text() if row.find('td') else ''
                if 'Release Date' in th_text:
                    year_match = re.search(r'\d{4}', td_text)
                    if year_match:
                        metadata['year'] = year_match.group(0)
                if 'Language' in th_text:
                    metadata['language'] = td_text.strip()
    return metadata

# Download and save book data including text and metadata
async def download_books(session, book, progress):
    book_id = book.a['href'].split('/')[-1]
    metadata = await get_book_metadata(session, book_id)
    if metadata['language'].lower() != 'english':
        progress.update(1)
        return None
    title = book.select_one('span.title').text if book.select_one('span.title') else "No Title"
    author = book.select_one('span.subtitle').text if book.select_one('span.subtitle') else "Unknown Author"
    text = await get_book_data(session, book_id)
    progress.update(1)
    if text:
        return {
            'ID': book_id,
            'Title': title,
            'Author': author,
            'Year': metadata['year'],
            'Text': text
        }

# Retrieve a list of books from Gutenberg's search results
async def get_books_list():
    index_url = "https://www.gutenberg.org/ebooks/search/?sort_order=downloads&languages=en"
    books = []
    processed_books = set()  # Set to keep track of processed book IDs
    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            text = await fetch_text(session, index_url)
            if text:
                soup = BeautifulSoup(text, 'html.parser')
                book_elements = soup.select('li.booklink')
                if not book_elements:
                    break
                progress = tqdm(total=len(book_elements), desc="Downloading books")
                tasks = []
                for book in book_elements:
                    book_id = book.a['href'].split('/')[-1]
                    if book_id not in processed_books:
                        tasks.append(download_books(session, book, progress))
                        processed_books.add(book_id)
                results = await asyncio.gather(*tasks)
                books.extend([result for result in results if result])
                progress.close()
                next_button = soup.find('a', string='Next')
                if next_button:
                    index_url = "https://www.gutenberg.org" + next_button['href']
                else:
                    break
    return books

# Clean the text to remove or replace characters that may cause issues
def clean_text(text):
    """Remove or replace illegal characters that might cause issues."""
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]+', '', text)
    text = text.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
    return text

# Save the books data to a TSV file
def save_to_csv(books):
    df = pd.DataFrame(books)
    df['Text'] = df['Text'].apply(clean_text)
    df.to_csv('gutenberg_books.tsv', sep='\t', index=False, encoding='utf-8', quotechar='"', quoting=csv.QUOTE_MINIMAL)

# Main entry point for the script
if __name__ == "__main__":
    if not os.path.exists('gutenberg_books.tsv'):
        books = asyncio.run(get_books_list())
        save_to_csv(books)
    else:
        print("Data already downloaded.")