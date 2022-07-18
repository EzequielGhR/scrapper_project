import scrapy

from bs4 import BeautifulSoup
from helper import *


class LaclspiderSpider(scrapy.Spider):
    name = 'LaClSpider'
    allowed_domains = ['cityclerk.lacity.org']
    start_urls = [
        'https://cityclerk.lacity.org/lacityclerkconnect/index.cfm?fa=ccfi.viewrecord&cfnumber=21-1247',
        'https://cityclerk.lacity.org/lacityclerkconnect/index.cfm?fa=ccfi.viewrecord&cfnumber=14-0694',
        'https://cityclerk.lacity.org/lacityclerkconnect/index.cfm?fa=ccfi.viewrecord&cfnumber=20-0631',
        'https://cityclerk.lacity.org/lacityclerkconnect/index.cfm?fa=ccfi.viewrecord&cfnumber=06-1293',
        'https://cityclerk.lacity.org/lacityclerkconnect/index.cfm?fa=ccfi.viewrecord&cfnumber=10-0180'
    ]

    custom_settings = {'FEEDS':{f'../extracted_raw/LaCLerk_spider_results.json':{'format':'json'}}}

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        file_id = response.url.rsplit('=', 1)[-1]
        yield dict(
            url=response.url,
            **get_summary(soup, file_id),
            vote_data=get_vote_info(soup),
            actions=get_events(soup),
            documents=get_documents(soup)   
        )