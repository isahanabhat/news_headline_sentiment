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

from news_headline_sentiment import news_scrape


class NewsScrapeBloomberg(news_scrape.NewsScraper):
    def __init__(self):
        bb_sitemap = 'https://www.bloomberg.com/sitemaps/news/index.xml'
        print("News Scraper (Bloomberg)")
        news_scrape.NewsScraper.__init__(self, 'bb')
        self.proxy_list = self.get_proxies()

    def get_proxies(self):
        url = 'https://free-proxy-list.net/anonymous-proxy.html'
        response = requests.get(url)
        parser = fromstring(response.text)
        proxies = set()
        for i in parser.xpath('//tbody/tr')[:100]:
            if i.xpath('.//td[7][contains(text(),"yes")]'):
                # Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
        return proxies

    def __bloomberg_check__(self, soup_data):
        tag_data = soup_data.find_all('meta')
        for t in tag_data:
            if t.get('content') == "games":
                return False
        return True

    def __retrieve_months__(self, date):
        date = parser.parse(date)
        month_url = []

        year_boundary = date.year
        month_boundary = date.month
        day_boundary = date.day
        cur_year = datetime.now().year
        cur_month = datetime.now().month
        base_url_bb = 'https://www.bloomberg.com/sitemaps/news/'
        while True:
            if year_boundary == cur_year and month_boundary == cur_month:
                month_url.append(base_url_bb + str(cur_year) + '-' + str(cur_month) + '.xml')
                break
            month_url.append(base_url_bb + str(cur_year) + '-' + str(cur_month) + '.xml')
            cur_month = ((cur_month - 1) % 12)
            if cur_month == 0:
                cur_month = 12
                cur_year -= 1
        return month_url

    """def __http_get__(self, url):
        self.__session_creator__()
        print("proxy count = ", len(self.proxy_list))
        for proxy in self.proxy_list:
            proxies = {
                'http': proxy,
                'https': proxy,
            }
            try:
                data = self.session.get(url, headers=self.HEADERS, proxies=proxies, verify=False)
                if data.find('title').text == 'Bloomberg - Are you a robot?':
                    print("Captcha encountered")
                else:
                    self.session_counter += 1
                    print("GET success!")
                    return data.content
            except Exception as e:
                print(str(e))
                print("Failed for page:", url, " with proxy ", proxy)
        print("Ran out of proxies")
    """

    def download_headlines(self, headline_count):
        # print('bloomberg here')
        news_file = pd.DataFrame()
        if os.path.exists(self.filepath):
            news_file = pd.read_csv(self.filepath)

        news_file = news_file.sort_values(by='headline', na_position='last')
        news_file_unprocessed = news_file[news_file['headline'].isna()]
        news_file_processed = news_file[~news_file['headline'].isna()]

        unprocessed_bb = news_file_unprocessed.loc[news_file['code'] == 'bb']
        unprocessed_remaining = news_file_unprocessed.loc[news_file['code'] != 'bb']
        n_downloaded = 0
        for row in unprocessed_bb.itertuples():
            if n_downloaded % 10 == 0:
                print("%d/%d" % (n_downloaded, headline_count))
            headline = (row.url).split('/')[-1]
            headline_parts = headline.split('-')

            h = lambda h_list: " ".join(h_list)
            unprocessed_bb.loc[row.Index, "headline"] = h(headline_parts)
            # print(h(headline_parts))
            n_downloaded += 1
            if n_downloaded == headline_count:
                break
        news_file_unprocessed = pd.concat([unprocessed_bb, unprocessed_remaining]).sort_index()
        news_file = pd.concat([news_file_processed, news_file_unprocessed]).sort_index()
        news_file = news_file.sort_values(by=['headline', 'last_modified', 'code'], ignore_index=True)
        news_file.to_csv(self.filepath, index=False)