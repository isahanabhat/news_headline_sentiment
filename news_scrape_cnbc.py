import os
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
import json

from news_headline_sentiment import news_scrape

class NewsScrapeCNBC(news_scrape.NewsScraper):
    def __init__(self):
        news_scrape.NewsScraper.__init__(self, 'cnbc')

        # CNBCsitemapAll12 has articles until 2023-09-11
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
        # CNBC groups all its articles and doesn't store by date
        # iterate 12th group, and stop when start date is reached

        t0 = time.time()
        root = self.__inital_sitemap__(self.cnbc_sitemap)

        for child in root:
            tags = re.split(r'url', child.tag)

            url_date = parser.parse(child.find(tags[0] + "lastmod").text)
            url_date = url_date.strftime('%Y-%m-%d')
            if self.__check_date_cnbc__(start_date, url_date):
                # print("start_date crossed")
                break

            element = tags[0] + "loc"

            retrieved_url = child.find(element).text
            if '.html' not in retrieved_url:
                continue

            if len(self.saved_data) != 0:
                if retrieved_url in self.saved_data.keys():  # change here
                    continue
            row = {
                'code': self.sitemap_code,
                'headline': numpy.nan,
                'last_extracted': datetime.today().strftime('%Y-%m-%d'),
                'last_modified': 'dt-' + url_date,
                'url': retrieved_url
            }
            self.rowlist[retrieved_url] = row

        new_rows = pd.DataFrame(self.rowlist.values())
        news_data = pd.concat([pd.DataFrame(self.saved_data.values()), new_rows], ignore_index=True).sort_values(by='last_modified', ascending=False)
        news_data.to_csv(self.filepath, index=False)

    def __retrieve_json_headline__(self, soup_data):
        json_str = soup_data.find('script', {'type': 'application/ld+json'}).text
        json_data = json.loads(json_str)
        # print("headline = ", json_data['headline'])
        return json_data['headline']

if __name__ == '__main__':
    print("CNBC")