import re
import pandas as pd
import logging

from bs4 import BeautifulSoup
from datetime import datetime as dt
from scrapy import Spider

BASE_URL = 'https://cityclerk.lacity.org/lacityclerkconnect/index.cfm?fa=ccfi.viewrecord&cfnumber={FILE_ID}'
FILE_IDS = ['21-1247', '14-0694', '20-0631', '06-1293', '10-0180']

def url_gen():
    for file_id in FILE_IDS:
        yield BASE_URL.format(FILE_ID=file_id)

def parse_date(raw:str) -> str:
    return pd.to_datetime(raw).strftime('%Y-%m-%d')

def clean_string(raw:str, lower:bool=False) -> str:
    out = re.sub(
        r"(?<=\d)(?=[A-Z][a-z])|(?<=[A-Z])(?=[A-Z][a-z])",
        "\n",
        re.sub(r'\s+', ' ', raw.strip())
    )
    if lower:
        return out.lower()
    return out

def _find_elements_by_text(soup:BeautifulSoup, text:str, table:bool=False, as_str:bool=True) -> str:
    element = soup.find('div', class_='reclabel', text=text)
    if not element:
        return ''
    element = element.find_next_sibling('div', class_='rectext')
    if not as_str:
        return element
    if 'date' in text.lower():
        return parse_date(element.text)
    if table:
        return element.find('table', id='inscrolltbl')
    return clean_string(element.text)

def get_summary(soup:BeautifulSoup, file_id:str) -> dict:
    f = _find_elements_by_text
    mover_elem = f(soup, 'Mover', as_str=False)
    second_elem = f(soup, 'Second', as_str=False)
    if mover_elem:
        movers = [
            clean_string(mv.text, lower=True) for mv in mover_elem.find_all('div')
        ]
    else:
        movers = []
    
    if second_elem:
        seconds = [
            clean_string(sc.text, lower=True) for sc in second_elem.find_all('div')
        ]
    else:
        seconds = []

    return dict(
        id=file_id,
        title=f(soup, 'Title'),
        date_received=f(soup, 'Date Received / Introduced'),
        last_modified=f(soup, 'Last Changed Date'),
        expiration=f(soup, 'Expiration Date'),
        reference=f(soup, 'Reference Numbers'),
        district=f(soup, 'Council District'),
        initiated_by=f(soup, 'Initiated by'),
        movers=movers,
        seconds=seconds
    )

def _get_document_reference(soup:BeautifulSoup, index:int) -> list:
    if index==0:
        return []
    tables = (
        soup
        .find('div', id=f'showtip_{index}')
        .find_all('table')
    )
    str_tables = []
    for table in tables:
        str_table = str(table)
        for a in table.find_all('a'):
            str_table = str_table.replace(str(a)+'</td>', a.text+'</td><td>'+a.attrs.get('href')+'</td>')
        str_tables.append(str_table)
    df = pd.concat(pd.read_html(''.join(str_tables)), ignore_index=True).rename(
        columns={0:'name', 1:'url', 2:'date'}
    )
    df['date'] = df.date.apply(parse_date)
    return df.to_dict(orient='records')

def get_events(soup:BeautifulSoup) -> list:
    table = _find_elements_by_text(soup, 'File Activities', table=True)
    str_table = str(table)
    for i, img in enumerate(table.find_all("img")):
        str_table = str_table.replace(str(img), str(i+1))

    activities_df = pd.read_html(str_table)[0].rename(
        columns={
            'Date':'date',
            'Activity':'activity',
            'Unnamed: 2':'document_index'
        }
    )
    activities_df['date'] = activities_df.date.apply(parse_date)
    activities_df['document_index'] = activities_df.document_index.fillna(0)
    activities_df = activities_df.astype({'document_index':int})
    activities_df['documents'] = activities_df['document_index'].apply(lambda ind: _get_document_reference(soup, ind))
    activities_df = activities_df.drop(columns=['document_index'])

    history = _find_elements_by_text(soup, 'File History', as_str=False)
    if history:
        lines = [br.previous_sibling for br in history.find_all('br')]
        rows = [line.split('- ', 1) + [[]] for line in lines]
        history_df = pd.DataFrame(columns=['date', 'activity', 'documents'], data = rows)
        history_df['date'] = history_df.date.apply(lambda date: (parse_date(
            date
            .strip()
            .replace('\n', '')
            .replace('\t', '')
            .replace('\r', '')
        )))
        history_df['activity'] = history_df.activity.apply(clean_string)
    else:
        history_df = pd.DataFrame(columns=['date', 'activity', 'documents'])
    return pd.concat([activities_df, history_df], ignore_index=True).to_dict(orient='records')

def get_documents(soup:BeautifulSoup) -> list:
    table = (
        soup
        .find('th', text='Title')
        .find_next('table')
    )
    str_table = str(table)
    for a in table.find_all('a'):
        str_table = str_table.replace(str(a)+'</td>', a.text+'</td><td>'+a.attrs.get('href')+'</td>')
    df = pd.read_html(str_table)[0].rename(columns={0:'name', 1:'url', 2:'date'})
    df['date'] = df.date.apply(parse_date)
    return df.to_dict(orient='records')

def _get_vote_summary(soup:BeautifulSoup) -> dict:
    summary = {}
    font_elem = soup.find('font', text='Council Vote Information')
    if clean_string(font_elem.find_next('div').text, lower=True) == 'no votes were found.':
        return {}
    rows = (
        soup
        .find('font', text='Council Vote Information')
        .find_next('table')
        .find_all('tr')
    )
    for tr in rows:
        key = tr.find('td')
        value = key.find_next_sibling('td')
        key = clean_string(key.text, lower=True).replace(' ', '_')[:-1]
        summary[key] = parse_date(clean_string(value.text)) if 'date' in key else clean_string(value.text)
    return summary

def get_vote_info(soup:BeautifulSoup) -> dict:
    summary = _get_vote_summary(soup)
    if not summary:
        return {}
    table = (
        soup
        .find('font', text='Council Vote Information')
        .find_next('table')
        .find_next_sibling('table')
    )
    df = pd.read_html(str(table))[0].rename(
        columns = {
            'Member Name': 'member_name',
            'CD': 'cd',
            'Vote': 'vote'
        }
    )
    df['member_name'] = df['member_name'].apply(lambda name: clean_string(name, lower=True))
    df['vote'] = df['vote'].apply(lambda vote: clean_string(vote, lower=True))
    return dict(
        **summary,
        members=df.to_dict(orient='records')
    )

def create_timestamp(format_:str='%Y-%m-%d_%H-%M-%S'):
    return dt.today().strftime(format_)