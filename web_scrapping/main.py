from web_scrapping.scraper import WebScraper
from web_scrapping.config import SCRAPER_URL, DATA_FOLDER


if __name__ == "__main__":
    scraper = WebScraper(SCRAPER_URL, DATA_FOLDER)
    scraper.run()