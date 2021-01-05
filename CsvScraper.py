from bs4 import BeautifulSoup
import requests
from requests.compat import urljoin
import re
import time
import os
import csv
import logging
from unidecode import unidecode

TIME_BETWEEN_REQUESTS = 5


class CsvScraper:

    def __init__(self, url):
        self.url = url

    def extract_links(self):
        content = requests.get(self.url).content
        soup = BeautifulSoup(content, "html.parser")
        csv_links = soup.find_all(href=re.compile(".csv$"))
        return [a['href'] for a in csv_links]

    def download_files(self, output_dir, check_existing=True):
        links = self.extract_links()
        os.makedirs(output_dir, exist_ok=True)
        for l in links:
            filename = os.path.join(output_dir, l)
            if check_existing and os.path.isfile(filename):
                logging.info(f"File {filename} already downloaded")
                continue
            url = urljoin(self.url, l)
            time.sleep(TIME_BETWEEN_REQUESTS)
            logging.info(f"Requesting {l}")
            response = requests.get(url)
            logging.info(f"Successfully downloaded")
            content = response.content.decode('iso_8859_1').split('\n')

            with open(filename, "w", newline='') as csv_file:
                writer = csv.writer(csv_file)
                reader = csv.reader(content)
                for row in reader:
                    if not row:
                        continue
                    decoded_row = map(unidecode, row)
                    writer.writerow(decoded_row)
            logging.info(f"File written to {filename}")
