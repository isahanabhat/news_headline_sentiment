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


    def download_headlines(self, to_download):
        news_file = pd.DataFrame()
        if os.path.exists(self.filepath):
            news_file = pd.read_csv(self.filepath)

        news_file = news_file.sort_values(by='headline', na_position='last')
        news_file_unprocessed = news_file[news_file['headline'].isna()]
        news_file_processed = news_file[~news_file['headline'].isna()]

        # unique_dates = news_file_unprocessed['last_modified'].unique()

        unique_dates_group = news_file_unprocessed.groupby('last_modified')
        news_file_unprocessed = pd.DataFrame()
        group_list = []
        flag = False
        for date, group in unique_dates_group:
            total_count = len(group)
            news_file_unprocessed = pd.DataFrame()
            n_downloaded = 0
            for row in group.itertuples():
                if n_downloaded % 10 == 0:
                    print(date, ": %d/%d" % (n_downloaded, to_download))
                headline = (row.url).split('/')
                if headline[-1] == "":
                    headline = headline[-2]
                else:
                    headline = headline[-1]
                headline_parts = headline.split('-')
                h = lambda h_list: " ".join(h_list)
                group.loc[row.Index, "headline"] = h(headline_parts)
                n_downloaded += 1
                total_count -= 1
                if n_downloaded == to_download:
                    print('saving')
                    group_list.append(group)
                    news_file_unprocessed = pd.concat(group_list).sort_index()
                    self.__save_headlines__(news_file_processed, news_file_unprocessed)
                    print()
                    break
                if total_count == 0:
                    print('saving')
                    group_list.append(group)
                    news_file_unprocessed = pd.concat(group_list).sort_index()
                    self.__save_headlines__(news_file_processed, news_file_unprocessed)
                    print()
        """news_file_unprocessed = pd.concat(group_list).sort_index()
        self.__save_headlines__(news_file_processed, news_file_unprocessed)"""
        return