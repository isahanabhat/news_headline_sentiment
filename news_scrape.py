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

from dateutil import parser

class NewsScraper:
	def __init__(self, filepath, sitemap_url, sitemap_code, start_date, article_count):
		self.sitemap_code = sitemap_code
		self.start_date = start_date
		self.article_count = article_count
		self.filepath = filepath

		print("Running...")

		# reading existing data from csv
		self.saved_data = {}
		if os.path.exists(self.filepath):
			file_data = pd.read_csv(filepath)
			file_data["url_index"] = file_data["url"]
			self.saved_data = file_data.set_index('url_index').to_dict('index')

		# retrieve sitemap data
		self.HEADERS = {
			"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
			'referrer': 'https://google.com',
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
			# 'Accept-Encoding': 'gzip, deflate, br',
			'Accept-Language': 'en-US,en;q=0.9',
			'Pragma': 'no-cache'
		}
		# create session
		self.session_counter = 0
		self.session = self.__session_creator__()

		self.root = self.__inital_sitemap__(sitemap_url) # root node of sitemap index xml tree

		# dictionary for new rows
		self.rowlist = {}

		self.month_url = self.__date_check_ap__(self.start_date, self.sitemap_code) # bloomberg

	def __session_creator__(self):
		return requests.session()

	def __inital_sitemap__(self, sitemap_url):
		data = self.session.get(sitemap_url, headers=self.HEADERS)
		self.session_counter += 1
		input = (data.content).decode("utf-8")
		data_tree = ET.ElementTree(ET.fromstring(input))
		return data_tree.getroot()  # root node of sitemap index xml tree

	def __date_check_ap__(self, date, site_code):
		date = parser.parse(date)
		month_url = []

		year_boundary = date.year
		month_boundary = date.month
		day_boundary = date.day
		cur_year = datetime.now().year
		cur_month = datetime.now().month
		base_url_ap = 'https://apnews.com/ap-sitemap-'
		if site_code == 'apnews':
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
				# print(base_url_ap + str(cur_year) + str(cur_month) + '.xml')
				cur_month = ((cur_month - 1) % 12)
				if cur_month == 0:
					cur_month = 12
					cur_year -= 1
		# print(month_url)
		return month_url
	

	def __site_crawler__(self, article_url):
		article_data = self.session.get(article_url, headers=self.HEADERS)
		self.session_counter += 1
		t3 = time.time()
		soup_data = BeautifulSoup(article_data.content, 'html.parser')
		return soup_data

	def __cnbc_check__(self, soup_data):
		if soup_data.find('meta').get('content') != "article":
			return False
		return True

	def __bloomberg_check__(self, soup_data):
		tag_data = soup_data.find_all('meta')
		for t in tag_data:
			if t.get('content') == "games":
				return False
		return True

	def __url_get__(self):
		i = 0
		j = 0
		t0 = time.time()
		# months = self.date_check_bb(self.start_date)
		for month in self.month_url:
			print("month = ", month)

			"""month_data = self.session.get(month, headers=self.HEADERS)
			# time.sleep(3)
			input = (month_data.content).decode("utf-8")
			data_tree = ET.ElementTree(ET.fromstring(input))
			month_root = data_tree.getroot()"""

			month_root = self.__inital_sitemap__(month)

			per_month = 0
			for child in month_root:

				# print("i = ",i)
				tags = re.split(r'url', child.tag)
				element = tags[0] + "loc"
				t1 = time.time()

				test_link = child.find(element).text
				t2 = time.time()

				"""soup_data = self.__site_crawler__(test_link)
				# time.sleep(3)
				t4 = time.time()

				if self.sitemap_code == 'apnews':
					# print(soup_data.find('html').get('class'))
					if soup_data.find('html').get('class') in [['AuthorPage'], ['TagPage'], ['StoryPage'], ['Page']]:
						continue

				if self.sitemap_code == 'cnbc':
					if not self.__cnbc_check__(soup_data):
						continue
				if self.sitemap_code == 'bb':
					if not self.__bloomberg_check__(soup_data):
						continue
				
				# row change here
				row = {
					'code': self.sitemap_code,
					# 'headline': soup_data.find('title').text,
					'headline': "",
				    # 'date_published': soup_data.find('template', attr={'data-date-tpl': True}),
				    'last_extracted': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
				    'last_modified': child.find(tags[0] + "lastmod").text,
				    'url': test_link
				    # 'type': soup_data.find('html').get('class')
					}
				"""

				row = {
					'code': self.sitemap_code,
					'headline': "",
					'last_extracted': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
					'last_modified': child.find(tags[0] + "lastmod").text,
					'url': test_link
				}

				if len(self.saved_data) != 0:
					rows_dict = list(self.saved_data.values())
					# head_list = [i['headline'] for i in rows_dict]
					url_list = [i['url'] for i in rows_dict]
					# if row['headline'] in head_list:  # change here
					if row['url'] in url_list:  # change here
						"""j += 1
						if j % 10 == 0:
							time.sleep(1)"""
						continue

				self.rowlist[test_link] = row
				# print(row['headline'], row['last_modified'])
				"""j += 1
				if j % 10 == 0:
					time.sleep(1)"""

				i += 1
				per_month += 1

				"""if i == self.article_count:
					break
				if per_month == 90:
					# print("here")
					break"""

		t10 = time.time()
		# avg_time_per_step = round((t10 - t0) / i, 4)
		# print("avg time = ", avg_time_per_step)


	# def save_to_file(self, filename):
		new_rows = pd.DataFrame(self.rowlist.values())
		news_data = pd.concat([pd.DataFrame(self.saved_data.values()), new_rows], ignore_index=True)

		# print(news_data)
		# print("time more than 1 = ", long_time)
		print(news_data)
		news_data.to_csv(self.filepath, index=False)

if __name__ == '__main__':
	print()
