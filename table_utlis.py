import datetime
from countries import Countries


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


def get_country_name(row):
    span = row.find('span')
    if 'ceFlags' in span.get('class'):
        country_name = span.get('title')
    else:
        country_name = ''
    return country_name


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
    res = row.find('td', class_='left event')
    if res:
        return res.text.strip()
    else:
        return ''


class Event:
    order = 'date,time,country_id,importance,event_text,actual,forecast,previous'

    def __init__(self, /, date='', time='', country_name='', importance='', event_text='', actual='', forecast='', previous='',
                 event_id=''):
        self.date = date
        self.time = time
        self.country_name = country_name
        self.importance = importance
        self.actual = actual
        self.forecast = forecast
        self.previous = previous
        self.event_text = event_text
        self.event_id = event_id
        self.country_id = Countries.get_country_code(self.country_name)


    def get_csv_row(self):
        columns_names = Event.order.split(',')
        row = [str(self.__dict__[col]) for col in columns_names]
        return row



def get_event(row):
    actual = ''
    forecast = ''
    previous = ''
    if not row.has_attr('id'):
        timestamp = int(row.find('td', class_='theDay').get('id')[6:])
        get_date_time.date = datetime.date.fromtimestamp(timestamp)
    else:
        event_id = row.attrs['id'].split('_')[1]
        event_text = get_event_text(row)
        date, time = get_date_time(row)
        country_name = get_country_name(row)
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
        return Event(date=date,
                     time=time,
                     country_name=country_name,
                     importance=importance,
                     event_text=event_text,
                     actual=actual,
                     forecast=forecast,
                     previous=previous,
                     event_id=event_id)
