import os.path
import cloudscraper
import csv
import datetime
import json
import logging
import asyncio
from aiocfscrape import CloudflareScraper
import utils
from countries import Countries, NotInitCountriesEx
from utils import handle_answer, get_logger, get_date_segments, get_continue_date


headers = {
    'content-type': 'application/x-www-form-urlencoded',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    'X-Requested-With': 'XMLHttpRequest',
    'referer': 'https://www.investing.com/economic-calendar/'
    }


def get_page_json(page_num, date_from='1970-01-01', date_to=None):
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
    return json_response


async def aio_get_events(date_from, date_to = None, csv_file = None):
    today_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    if not csv_file:
        csv_file = f'{date_from}.csv'
    url = 'https://www.investing.com/economic-calendar/Service/getCalendarFilteredData'
    data = {
        'dateFrom': date_from,
        'dateTo': today_date_str if not date_to else date_to,
        'timeZone': '8',
        'timeFilter': 'timeRemain',
        'currentTab': 'custom',
        'submitFilters': '1',
        'limit_from': '0',
        'country[]': Countries.codes,
    }
    async with CloudflareScraper() as session:
        while True:
            async with session.post(url, data=data, headers=headers) as response:
                txt = await response.text()
                jsn = json.loads(txt)
                pids = jsn['pids']
                if not pids:
                    break
                try:
                    with open(csv_file, 'a', encoding='UTF-8', newline='') as file:
                        print(data['dateFrom'], data['dateTo'], data['limit_from'])
                        writer = csv.writer(file)
                        parsed_data_rows = handle_answer(jsn['data'])
                        writer.writerows(parsed_data_rows)
                except NotInitCountriesEx:
                    raise NotInitCountriesEx
                except Exception as e:
                    log = logging.getLogger('parser')
                    log.error(f'{data["dateFrom"]}, {data["dateTo"]}, {data["limit_from"]}')
                    log.error(e)
                data['limit_from'] = int(data['limit_from']) + 1


async def aiostart(date_from, date_to, threads = 5, output_csv='result.csv'):
    dates_from, dates_to = get_date_segments(threads, date_from, date_to)
    tasks = []
    for f,t in zip(dates_from, dates_to):
        tasks.append(aio_get_events(f, t))
    await asyncio.gather(*tasks)
    utils.unite_csvs(out_file_name=output_csv, delete_source=False)


def sync_start(date_from='1970-01-01', date_to='', csv_result='result.csv', csv_countries='countries.csv'):
    log = get_logger()
    Countries.load(csv_countries)
    query_page_index = 0
    if os.path.exists(csv_result):
        date_from = get_continue_date(csv_result)
    with open(csv_result, 'a', encoding='UTF-8', newline='') as output_csv:
        writer = csv.writer(output_csv)
        while True:
            try:
                print('Page ' + str(query_page_index))
                answer = get_page_json(query_page_index, date_from, date_to)
                if not answer.get('pids', None):
                    print('Finished')
                    break
                parsed_data = handle_answer(answer['data'])
                writer.writerows(parsed_data)
            except Exception as e:
                log.warning(f'Page not processed: {query_page_index}', exc_info=e)
            query_page_index += 1


if __name__ == '__min__':
    sync_start(date_from='1970-01-01', date_to='2022-11-01', csv_result='result.csv', csv_countries='countries.csv')


if __name__ == '__main__':
    get_logger()
    Countries.load('countries.csv')
    asyncio.run(aiostart('1970-01-01', '2022-11-01', output_csv='results.csv'))


