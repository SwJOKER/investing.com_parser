import cloudscraper
from bs4 import BeautifulSoup
import csv
import datetime
import json
import os
import logging


titles_csv = 'date,time,country_id,importance,event_text,actual,forecast,previous\n'


headers = {
    'content-type': 'application/x-www-form-urlencoded',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    'X-Requested-With': 'XMLHttpRequest',
    'referer': 'https://www.investing.com/economic-calendar/'
    }


class StopRequestEx(Exception):
    def __init__(self):
        Exception.__init__(self, 'All data recieved')


class Countries:
    """Импорт списка стран с сайта в csv"""
    dict = {}
    codes = []

    @classmethod
    def init(cls, csv_path):
        Countries.csv_path = csv_path
        if not os.path.exists(Countries.csv_path):
            Countries.countries_to_csv()
        Countries.init_countries_dict()
        Countries.codes = Countries.get_country_codes()

    @classmethod
    def countries_to_csv(cls):
        scraper = cloudscraper.create_scraper()
        page_text = scraper.get('https://www.investing.com/economic-calendar/').text
        try:
            with open(Countries.csv_path, 'w', encoding='UTF-8', newline='') as file:
                soup = BeautifulSoup(page_text, features='lxml')
                countries_data = soup.find('ul', class_='countryOption').find_all('li')
                csv_w = csv.writer(file)
                csv_w.writerow(['Title','Code'])
                for country in countries_data:
                    country_id = country.find('input').get('value')
                    # Срез убирает паразитный символ в начале строки
                    csv_w.writerow([country.text[1:], country_id])
        except:
            os.remove(Countries.csv_path)

    @classmethod
    def init_countries_dict(cls):
        with open(Countries.csv_path, 'r', encoding='UTF-8') as file:
            file.readline() #пропустить строку заголовка
            reader = csv.reader(file)
            for x in reader:
                print(x)
                Countries.dict[x[0]] = int(x[1])

    @classmethod
    def get_country_codes(cls):
        with open(Countries.csv_path, 'r', encoding='UTF-8') as file:
            file.readline() #пропустить строку заголовка
            reader = csv.reader(file)
            codes = [int(x[1]) for x in reader]
            return codes


def get_date_time(row):
    date_time_raw = row.get('data-event-datetime', None)
    if date_time_raw:
        date_time_formatted = datetime.datetime.strptime(date_time_raw, '%Y/%m/%d %H:%M:%S')
        date = date_time_formatted.strftime('%Y-%m-%d')
        time = date_time_formatted.strftime('%H:%M:%S')
    else:
        date = get_date_time.date
        time = row.find('td', class_='first left').text
    return date, time


def get_country_id(row):
    span = row.find('span')
    if 'ceFlags' in span.get('class'):
        return Countries.dict[span.get('title')]


def get_importance(row):
    tds = row.find_all('td')
    target = tds[2]
    result = len(target.find_all('i', class_='grayFullBullishIcon'))
    if not result:
        try:
            result = target.find('span').text
        except AttributeError:
            result = ''
    return result


def get_event_text(row):
    res = row.find('td', class_='left event').text
    return res.strip()


def get_all_data(row):
    date = ''
    time = ''
    country_id = ''
    importance = ''
    actual = ''
    forecast = ''
    previous = ''
    event = ''
    if not row.has_attr('id'):
        timestamp = int(row.find('td', class_='theDay').get('id')[6:])
        get_date_time.date = datetime.date.fromtimestamp(timestamp)
    else:
        event_id = row.attrs['id'].split('_')[1]
        event_text = get_event_text(row)
        date, time = get_date_time(row)
        country_id = get_country_id(row)
        importance = get_importance(row)
        get_event_text(row)
        tds = row.find_all('td')
        for td in tds:
            if td.has_attr('id'):
                if td.get('id').startswith('eventActual'):
                    actual = td.text.replace('\xa0', '')
                if td.get('id').startswith('eventForecast'):
                    forecast = td.text.replace('\xa0', '')
                if td.get('id').startswith('eventPrevious'):
                    previous = td.text.replace('\xa0', '')
        return [date, time, country_id, importance, event_text, actual, forecast, previous]


def handle_answer(page_html, csv_file):
    writer = csv.writer(csv_file)
    soup = BeautifulSoup(page_html, 'lxml')
    trs = soup.find_all('tr')
    rows = []
    for tr in trs:
        if res := get_all_data(tr):
            rows.append(res)
    for row in rows:
        writer.writerow(row)


def get_page(page_num, date_from='1970-01-01', date_to=None):
    today_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    url = 'https://www.investing.com/economic-calendar/Service/getCalendarFilteredData'
    data = {
        'dateFrom': date_from,
        'dateTo': today_date_str if not date_to else date_to,
        'timeZone': '8',
        'timeFilter': 'timeRemain',
        'currentTab': 'custom',
        'submitFilters': '1',
        'limit_from': str(page_num),
        'country[]': Countries.codes,
    }
    scraper = cloudscraper.create_scraper()
    with scraper.post(url, data=data, headers=headers) as response:
        if response.status_code != 200:
            raise Exception(f'Response code: {response.status_code}')
        json_response = json.loads(response.text)
    if not json_response['pids']:
        raise StopRequestEx
    return json_response['data']


def get_continue_date(csv_path):
    last_date = '1970-01-01'
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='UTF-8') as csv_f:
            lines = csv_f.readlines()[1:]
            if not lines:
                return last_date
            n = 1
            while not last_date:
                last_date = lines[-n].split(',')[0]
                n += 1
        with open(csv_path, 'w', encoding='UTF-8') as csv_f:
            csv_f.write(titles_csv)
            for line in lines:
                if line.split(',')[0] != last_date:
                    csv_f.write(line)
    else:
        with open(csv_path, 'w', encoding='UTF-8') as csv_f:
            csv_f.write(titles_csv)
    return last_date


def get_logger():
    log = logging.getLogger('Parser')
    file_handler = logging.FileHandler('parser.log', encoding='UTF-8')
    file_handler.setLevel(logging.WARNING)
    info_handler = logging.StreamHandler()
    info_handler.setLevel(logging.WARNING)
    log.addHandler(file_handler)
    log.addHandler(info_handler)
    return log


if __name__ == '__main__':
    log = get_logger()
    try:
        Countries.init('countries.csv')
        query_page_index = 0
        result_csv_path = 'res.csv'
        date_from = get_continue_date(result_csv_path)
        with open('res.csv', 'a', encoding='UTF-8', newline='') as output_csv:
            while True:
                try:
                    answer = get_page(query_page_index, date_from)
                    print('Page ' + str(query_page_index))
                    handle_answer(answer, output_csv)
                except StopRequestEx:
                    raise StopRequestEx
                except Exception as e:
                    log.warning(f'Page not processed: {query_page_index}', exc_info=e)
                query_page_index += 1
    except StopRequestEx:
        print('Finish')
