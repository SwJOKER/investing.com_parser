import logging
import os
from table_utlis import Event, get_event
import datetime
from bs4 import BeautifulSoup
import re


def get_logger():
    log = logging.getLogger('parser')
    file_handler = logging.FileHandler('parser.log', encoding='UTF-8')
    file_handler.setLevel(logging.WARNING)
    info_handler = logging.StreamHandler()
    info_handler.setLevel(logging.WARNING)
    log.addHandler(file_handler)
    log.addHandler(info_handler)
    return log


def get_continue_date(csv_path):
    last_date = None
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
            csv_f.write(Event.order)
            for line in lines:
                if line.split(',')[0] != last_date:
                    csv_f.write(line)
    else:
        with open(csv_path, 'w', encoding='UTF-8') as csv_f:
            csv_f.write(Event.order)
    return last_date


def get_date_segments(segments: int, dates_from: str, date_to: str) -> tuple[list[str], list[str]]:
    """divides the time intervals to segments for the request"""
    dfrom = datetime.datetime.strptime(dates_from, '%Y-%m-%d')
    dto = datetime.datetime.strptime(date_to, '%Y-%m-%d')
    interval = (dto - dfrom) / segments
    dates_to = []
    dates_from = [dates_from]
    for x in range(1,5):
        day = datetime.timedelta(days=1)
        dto = (dfrom + interval*x).date()
        dates_to.append(str(dto))
        dates_from.append(str(dto + day))
    dates_to.append(date_to)
    return dates_from, dates_to


def handle_answer(page_html):
    soup = BeautifulSoup(page_html, 'lxml')
    trs = soup.find_all('tr')
    rows = []
    for tr in trs:
        if event := get_event(tr):
            rows.append(event.get_csv_row())
    return rows


def unite_csvs(regex='\d{4}-\d{2}-\d{2}.csv',out_file_name='results.csv', directory='', delete_source=True):
    files = []
    reg = re.compile(regex)
    if not directory:
        directory = os.curdir
    for i in os.listdir(directory):
        if reg.match(i):
            files.append(i)
    lines = []
    files.sort()
    with open(out_file_name, 'w+', encoding='UTF-8') as out_file:
        for file in files:
            with open(file, 'r', encoding='UTF-8') as cur_file:
                lines.extend(cur_file.readlines())
        lines.sort(key=lambda x: x.split(',')[0])
        out_file.write(Event.order)
        out_file.write('\n')
        out_file.writelines(lines)
    if delete_source:
        for file in files:
            os.remove(file)

