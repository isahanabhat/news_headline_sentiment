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

    def __retrieve_months_bb__(self, date):
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

    def __http_get__(self, url):
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

    def __beautiful_soup_from_site__(self, article_url, proxies):
        article_data = self.__http_get__(article_url, proxies)
        t3 = time.time()
        soup_data = BeautifulSoup(article_data, 'html.parser')
        return soup_data

    def url_getall(self, start_date):
        i = 0
        t0 = time.time()
        month_url = self.__retrieve_months_bb__(start_date)
        for month in month_url:
            print("month = ", month)
            month_root = self.__inital_sitemap__(month)

            for child in month_root:
                tags = re.split(r'url', child.tag)
                element = tags[0] + "loc"
                t1 = time.time()

                retrieved_url = child.find(element).text
                t2 = time.time()

                if len(self.saved_data) != 0:
                    if retrieved_url in self.saved_data.keys():  # change here
                        i += 1
                        continue
                row = {
                    'code': self.sitemap_code,
                    'headline': numpy.nan,
                    'last_extracted': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                    'last_modified': child.find(tags[0] + "lastmod").text,
                    'url': retrieved_url
                }

                self.rowlist[retrieved_url] = row
                i += 1

        t10 = time.time()
        new_rows = pd.DataFrame(self.rowlist.values())
        news_data = pd.concat([pd.DataFrame(self.saved_data.values()), new_rows], ignore_index=True).sort_values(
            by='last_modified', ascending=False)
        news_data.to_csv(self.filepath, index=False)

    def download_headlines(self, headline_count):
        news_file = pd.DataFrame()
        if os.path.exists(self.filepath):
            news_file = pd.read_csv(self.filepath)

        news_file = news_file.sort_values(by='headline', na_position='last')
        news_file_unprocessed = news_file[news_file['headline'].isna()]
        news_file_processed = news_file[~news_file['headline'].isna()]
        n_downloaded = 0
        for row in news_file_unprocessed.itertuples():
            if n_downloaded % 10 == 0:
                print("%d/%d" % (n_downloaded, headline_count))
            headline = (row.url).split('/')[-1]
            headline_parts = headline.split('-')

            h = lambda h_list: " ".join(h_list)
            news_file_unprocessed.loc[row.Index, "headline"] = h(headline_parts)
            n_downloaded += 1
            if n_downloaded == headline_count:
                break
        news_file = pd.concat([news_file_processed, news_file_unprocessed]).sort_index()
        news_file.to_csv(self.filepath, index=False)