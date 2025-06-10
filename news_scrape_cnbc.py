import os
from encodings.idna import sace_prefix
import requests
import re
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import html5lib
import pandas as pd
from datetime import datetime
import time
import numpy
from dateutil import parser
from lxml.html import fromstring
import json

from news_headline_sentiment import news_scrape

class NewsScrapeCNBC(news_scrape.NewsScraper):
    def __init__(self):
        news_scrape.NewsScraper.__init__(self, 'cnbc')
        self.cnbc_sitemap = 'https://www.cnbc.com/CNBCsitemapAll12.xml' # subject to change
        print("News Scraper (CNBC)")

    def __cnbc_check__(self, soup_data):
        if soup_data.find('meta').get('content') != "article":
            return False
        return True

    def __check_date_cnbc__(self, start_date, url_date):
        start_date = parser.parse(start_date, ignoretz=True)
        url_date = parser.parse(url_date, ignoretz=True)
        # whether the date limit from the user comes before the current url's publishing date (FALSE)
        # when the date limit has been crossed, dont take that url's information (TRUE)
        return start_date > url_date

    def url_getall(self, start_date):
        i = 0
        t0 = time.time()
        root = self.__inital_sitemap__(self.cnbc_sitemap)

        for child in root:
            tags = re.split(r'url', child.tag)

            url_date = prser.parse(child.find(tags[0] + "lastmod").text)
            url_date = url_date.strftime('%Y-%m-%d')
            if self.__check_date_cnbc__(start_date, url_date):
                print("start_date crossed")
                break

            element = tags[0] + "loc"
            t1 = time.time()

            retrieved_url = child.find(element).text
            if '.html' not in retrieved_url:
                continue

            t2 = time.time()

            if len(self.saved_data) != 0:
                if retrieved_url in self.saved_data.keys():  # change here
                    i += 1
                    continue
            row = {
                'code': self.sitemap_code,
                'headline': numpy.nan,
                'last_extracted': datetime.today().strftime('%Y-%m-%d'),
                'last_modified': url_date,
                'url': retrieved_url
            }

            self.rowlist[retrieved_url] = row
            i += 1

        t10 = time.time()
        new_rows = pd.DataFrame(self.rowlist.values())
        news_data = pd.concat([pd.DataFrame(self.saved_data.values()), new_rows], ignore_index=True).sort_values(by='last_modified', ascending=False)
        news_data.to_csv(self.filepath, index=False)

    def __retrieve_json_headline__(self, soup_data):
        # print('cnbc func')
        json_str = soup_data.find('script', {'type': 'application/ld+json'}).text
        json_data = json.loads(json_str)
        # print("headline = ", json_data['headline'])
        return json_data['headline']