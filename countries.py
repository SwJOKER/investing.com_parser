import csv
import cloudscraper
from bs4 import BeautifulSoup
import os


class NotInitCountriesEx(Exception):
    def __init__(self):
        super().__init__('Countries not loaded. <Countries.load>')


class Countries:
    """Import country list to csv"""
    dict = {}
    codes = []

    loaded = False

    @classmethod
    def load(cls, csv_path):
        """Download country list to csv. If csv exists, loads list from it"""
        Countries.csv_path = csv_path
        if not os.path.exists(Countries.csv_path):
            Countries.countries_to_csv()
        Countries.init_countries_dict()
        Countries.codes = Countries.get_country_codes()
        Countries.loaded = True

    @classmethod
    def countries_to_csv(cls):
        """Create csv and write country info to it"""
        scraper = cloudscraper.create_scraper()
        page_text = scraper.get('https://www.investing.com/economic-calendar/').text
        try:
            with open(Countries.csv_path, 'w', encoding='UTF-8', newline='') as file:
                soup = BeautifulSoup(page_text, features='lxml')
                countries_data = soup.find('ul', class_='countryOption').find_all('li')
                csv_w = csv.writer(file)
                csv_w.writerow(['Title', 'Code'])
                for country in countries_data:
                    country_id = country.find('input').get('value')
                    # slice deleting parasite char in start of string
                    csv_w.writerow([country.text[1:], country_id])
        except Exception:
            os.remove(Countries.csv_path)

    @classmethod
    def init_countries_dict(cls):
        """Load country dictionary from csv"""
        with open(Countries.csv_path, 'r', encoding='UTF-8') as file:
            # skip title line
            file.readline()
            reader = csv.reader(file)
            for x in reader:
                Countries.dict[x[0]] = int(x[1])

    @classmethod
    def get_country_codes(cls) -> list[int]:
        """Load all country codes list from csv"""
        with open(Countries.csv_path, 'r', encoding='UTF-8') as file:
            # skip title line
            file.readline()
            reader = csv.reader(file)
            codes = [int(x[1]) for x in reader]
            return codes

    @classmethod
    def get_country_code(cls, name) -> int:
        """Change country name to code"""
        if not Countries.loaded:
            raise NotInitCountriesEx
        try:
            return Countries.dict[name]
        except KeyError:
            raise KeyError(f'Not enough data in {Countries.csv_path}')
