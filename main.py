from Aggregator import Aggregator
from CsvScraper import CsvScraper
from Pre2017Extractor import Pre2017Extractor
from Post2017Extractor import Post2017Extractor
import logging

ELECTION_YEARS = range(2002, 2021, 3)
DATA_DIR = "./data"
OUTPUT_DIR = "./output"


def get_csv_url(year):
    if year < 2017:
        return f"https://www.electionresults.govt.nz/electionresults_{year}/e9/csv/"
    return f"https://www.electionresults.govt.nz/electionresults_{year}/statistics/csv/"


def aggregate():
    ag = Aggregator(ELECTION_YEARS, OUTPUT_DIR)
    ag.aggregate_all()


def download():
    for year in ELECTION_YEARS:
        csv_url = get_csv_url(year)
        scraper = CsvScraper(csv_url)
        scraper.download_files(f"{DATA_DIR}/{year}")


def process():
    for year in ELECTION_YEARS:
        if year < 2017:
            extractor = Pre2017Extractor(year, DATA_DIR, OUTPUT_DIR)
        else:
            extractor = Post2017Extractor(year, DATA_DIR, OUTPUT_DIR)
        extractor.extract_all()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    aggregate()
