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
import json

from dateutil import parser


class NewsScraper:
    def __init__(self, sitemap_code):
        file = r"news_data.csv"
        DATA_PATH = os.getenv('DATA_HOME')
        self.filepath = os.path.join(DATA_PATH, file)

        self.sitemap_code = sitemap_code

        print("Running...")

        # reading existing data from csv
        self.saved_data = {}
        if os.path.exists(self.filepath):
            file_data = pd.read_csv(self.filepath)
            file_data["url_index"] = file_data["url"]
            self.saved_data = file_data.set_index('url_index').to_dict('index')

        # retrieve sitemap data
        self.HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            'referrer': 'https://google.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Pragma': 'no-cache'
        }
        # create session
        self.session_counter = 0
        self.session = None

        # dictionary for new rows
        self.rowlist = {}

    def __session_creator__(self):
        if self.session_counter >= 50:
            self.session.close()
            self.session_counter = 0
            self.session = requests.session()
        elif self.session is None:
            self.session = requests.session()

    def __http_get__(self, url):
        self.__session_creator__()
        data = self.session.get(url, headers=self.HEADERS)
        self.session_counter += 1
        return data.content

    def __inital_sitemap__(self, sitemap_url):
        data = self.__http_get__(sitemap_url)
        input = (data).decode("utf-8")
        data_tree = ET.ElementTree(ET.fromstring(input))
        return data_tree.getroot()  # root node of sitemap index xml tree

    def __retrieve_months_ap__(self, date):
        date = parser.parse(date)
        month_url = []

        year_boundary = date.year
        month_boundary = date.month
        day_boundary = date.day
        cur_year = datetime.now().year
        cur_month = datetime.now().month
        base_url_ap = 'https://apnews.com/ap-sitemap-'
        while True:
            if year_boundary == cur_year and month_boundary == cur_month:
                if len(str(cur_month)) == 2:
                    month_url.append(base_url_ap + str(cur_year) + str(cur_month) + '.xml')
                else:
                    month_url.append(base_url_ap + str(cur_year) + '0' + str(cur_month) + '.xml')
                break

            if len(str(cur_month)) == 2:
                month_url.append(base_url_ap + str(cur_year) + str(cur_month) + '.xml')
            else:
                month_url.append(base_url_ap + str(cur_year) + '0' + str(cur_month) + '.xml')
            cur_month = ((cur_month - 1) % 12)
            if cur_month == 0:
                cur_month = 12
                cur_year -= 1
        return month_url

    def __beautiful_soup_from_site__(self, article_url):
        article_data = self.__http_get__(article_url)
        t3 = time.time()
        soup_data = BeautifulSoup(article_data, 'html.parser')
        return soup_data


    def url_getall(self, start_date):
        i = 0
        t0 = time.time()
        month_url = self.__retrieve_months_ap__(start_date)
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
        news_data = pd.concat([pd.DataFrame(self.saved_data.values()), new_rows], ignore_index=True).sort_values(by='last_modified', ascending=False)
        news_data.to_csv(self.filepath, index=False)

    def __retrieve_json_headline__(self, soup_data):
        json_str = soup_data.find('script', {'id': 'link-ld-json'}).text[1:-1]
        json_data = json.loads(json_str)
        return json_data['headline']

    def download_headlines(self, headline_count):
        news_file = pd.DataFrame()
        if os.path.exists(self.filepath):
            news_file = pd.read_csv(self.filepath)

        news_file = news_file.sort_values(by='headline', na_position='last')
        news_file_unprocessed = news_file.loc[news_file['headline'].isna()]
        news_file_processed = news_file.loc[~news_file['headline'].isna()]

        unprocessed_ap = news_file_unprocessed.loc[news_file['code'] == self.sitemap_code]
        unprocessed_remaining = news_file_unprocessed.loc[news_file['code'] != self.sitemap_code]

        n_downloaded = 0
        for row in unprocessed_ap.itertuples():
            if n_downloaded % 10 == 0:
                print("%d/%d" %(n_downloaded, headline_count))
            headline = ''
            try:
                soup_data = self.__beautiful_soup_from_site__(row.url)
                headline = self.__retrieve_json_headline__(soup_data)
            except Exception as e:
                headline = soup_data.find('title').text

            unprocessed_ap.loc[row.Index, "headline"] = headline
            n_downloaded += 1
            if n_downloaded == headline_count:
                break
        news_file_unprocessed =pd.concat([unprocessed_ap, unprocessed_remaining]).sort_index()
        news_file = pd.concat([news_file_processed, news_file_unprocessed]).sort_index()
        news_file.to_csv(self.filepath, index=False)
        return


if __name__ == '__main__':
    print()
