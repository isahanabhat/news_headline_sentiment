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

class NewsScrapeAPNews(news_scrape.NewsScraper):
    def __init__(self):
        news_scrape.NewsScraper.__init__(self, 'apnews')
        self.ap_sitemap = "https://apnews.com/ap-sitemap.xml"
        print("News Scraper (AP News)")

    def __retrieve_months__(self, date):
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

