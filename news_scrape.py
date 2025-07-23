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
import json

from dateutil import parser


class NewsScraper:
    def __init__(self, sitemap_code):
        self.sitemap_code = sitemap_code

        file = "headlines_data_" + self.sitemap_code + ".csv"
        DATA_PATH = os.getenv('DATA_HOME')
        self.filepath = os.path.join(DATA_PATH, file)

        # reading existing data from csv
        self.saved_data = {}
        if os.path.exists(self.filepath):
            file_data = pd.read_csv(self.filepath, dtype=str)
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

    def __conver_date__(self, date_string):
        # convert the date string to YYYY-mm-dd
        if date_string is None or date_string == "" or pd.isnull(date_string):
            return np.nan
        date_string = parser.parse(date_string)
        return date_string.strftime('%Y-%m-%d')

    def __session_creator__(self):
        # limit the retrievals to 50 per session, then create a new session object
        if self.session_counter >= 50:
            self.session.close()
            self.session_counter = 0
            self.session = requests.session()
        elif self.session is None:
            self.session = requests.session()

    def __http_get__(self, url):
        try:
            self.__session_creator__()
            data = self.session.get(url, headers=self.HEADERS) # retrieve the page source of url
            self.session_counter += 1
            return data.content
        except Exception as e:
            print(str(e))
            return None

    def __inital_sitemap__(self, sitemap_url):
        data = self.__http_get__(sitemap_url)
        input = (data).decode("utf-8")
        data_tree = ET.ElementTree(ET.fromstring(input))
        return data_tree.getroot()  # root node of sitemap index xml tree

    def __retrieve_months__(self, date):
        pass


    def __beautiful_soup_from_site__(self, article_url):
        article_data = self.__http_get__(article_url)
        t3 = time.time()
        soup_data = BeautifulSoup(article_data, 'html.parser')
        return soup_data


    def url_getall(self, start_date):
        month_url = self.__retrieve_months__(start_date)
        for month in month_url:
            month_root = self.__inital_sitemap__(month) # retrieve sitemap root
            t0 = time.time()
            for child in month_root:
                # split the current element of the tree to retrieve url part,
                # concatenate with "loc", and then find the url with this string
                tags = re.split(r'url', child.tag)
                element = tags[0] + "loc"
                retrieved_url = child.find(element).text

                # ensure that the url isn't already retrieved
                if len(self.saved_data) != 0:
                    if retrieved_url in self.saved_data.keys():
                        continue

                last_modified = parser.parse(child.find(tags[0] + "lastmod").text)
                row = {
                    'code': self.sitemap_code,
                    'headline': numpy.nan,
                    'last_extracted': datetime.today().strftime('%Y-%m-%d'),
                    'last_modified': 'dt-'+last_modified.strftime('%Y-%m-%d'),
                    'url': retrieved_url
                }
                self.rowlist[retrieved_url] = row
            t1 = time.time()
            print("month retrieved = ", month)
            print("time take = ", t1 - t0)
            print()

        new_rows = pd.DataFrame(self.rowlist.values())
        news_data = pd.concat([pd.DataFrame(self.saved_data.values()), new_rows], ignore_index=True).sort_values(by='last_modified', ascending=False)
        news_data.to_csv(self.filepath, index=False)

    def __retrieve_json_headline__(self, soup_data):
        # function to retrieve the headline from the JSON string in the page source <script> content
        json_str = soup_data.find('script', {'id': 'link-ld-json'}).text
        if json_str[0] == "[" and json_str[-1] == "]":
            json_str = json_str[1:-1]
        json_data = json.loads(json_str)
        return json_data['headline']

    def __save_headlines__(self, df_1, df_2):
        df = pd.concat([df_1, df_2]).sort_index()
        df = df.sort_values(by=['headline'], ignore_index=True)
        df.to_csv(self.filepath, index=False)

    def download_headlines(self, to_download):
        # function retrives a particular number of headlines (to_download) per day
        news_file = pd.DataFrame()
        if os.path.exists(self.filepath):
            news_file = pd.read_csv(self.filepath)

        news_file = news_file.sort_values(by='headline', na_position='last')
        news_file_unprocessed = news_file.loc[news_file['headline'].isna()] # dataframe of not retrieved headlines
        news_file_processed = news_file.loc[~news_file['headline'].isna()] # dataframe of retrieved headlines

        unique_dates = news_file_unprocessed['last_modified'].unique()
        unique_dates_group = news_file_unprocessed.groupby('last_modified') # group unprocessed dataframe by date
        news_file_unprocessed = pd.DataFrame()
        group_list = []
        total_count = 0
        rev_keys = sorted(unique_dates_group.groups.keys(), reverse=True)

        # download data from last date till the first date of url's in dataframe
        for date in rev_keys:
            group = unique_dates_group.get_group(date)
            n_downloaded = 0
            total_count = len(group)
            for row in group.itertuples():
                if n_downloaded % 10 == 0:
                    print(date, ": %d/%d" % (n_downloaded, to_download))
                headline = ''
                try:
                    soup_data = self.__beautiful_soup_from_site__(row.url)
                    headline = self.__retrieve_json_headline__(soup_data)
                except Exception as e:
                    print('================================')
                    print("EXCEPTION:", str(e))
                    # retrieve headline with find() in case of exception
                    headline = soup_data.find('title').text
                    print('================================')
                group.loc[row.Index, "headline"] = headline
                n_downloaded += 1
                total_count -= 1

                # save the data in dataframe as soon as to_download count has been reached
                if n_downloaded == to_download:
                    print('saving')
                    group_list.append(group)
                    news_file_unprocessed = pd.concat(group_list).sort_index()
                    self.__save_headlines__(news_file_processed, news_file_unprocessed)
                    break
                if total_count == 0:
                    print('saving')
                    group_list.append(group)
                    news_file_unprocessed = pd.concat(group_list).sort_index()
                    self.__save_headlines__(news_file_processed, news_file_unprocessed)
                    print()

        news_file_unprocessed = pd.concat(group_list).sort_index()
        self.__save_headlines__(news_file_processed, news_file_unprocessed)
        return

if __name__ == '__main__':
    print()