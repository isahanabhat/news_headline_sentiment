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
			"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
		}
		data = requests.get(sitemap_url, headers=self.HEADERS)
		input = (data.content).decode("utf-8")
		data_tree = ET.ElementTree(ET.fromstring(input))
		self.root = data_tree.getroot() # root node of sitemap xml tree

		# create session
		self.session = requests.session()

		# dictionary for new rows
		self.rowlist = {}

	def site_crawler(self, article_url):
		article_data = self.session.get(article_url, headers=self.HEADERS)
		t3 = time.time()
		soup_data = BeautifulSoup(article_data.content, 'html.parser')
		return soup_data

	def cnbc_check(self, soup_data):
		if soup_data.find('meta').get('content') != "article":
			return False
		return True

	def bloomberg_check(self, soup_data):
		tag_data = soup_data.find_all('meta')
		for t in tag_data:
			if t.get('content') == "games":
				return False
		return True

	def get_data(self):
		i = 0
		j = 0
		t0 = time.time()

		for child in self.root:
			print("i = ",i)
			tags = re.split(r'url', child.tag)
			element = tags[0] + "loc"
			t1 = time.time()

			test_link = child.find(element).text
			t2 = time.time()

			soup_data = self.site_crawler(test_link)
			t4 = time.time()

			if self.sitemap_code == 'cnbc':
				if not self.cnbc_check(soup_data):
					continue
			if self.sitemap_code == 'bb':
				if not self.bloomberg_check(soup_data):
					continue

			# row change here
			row = {'code': self.sitemap_code,
				   'headline': soup_data.find('title').text,
				   # 'date_published': soup_data.find('template', attr={'data-date-tpl': True}),
				   'last_extracted': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
				   'last_modified': child.find(tags[0] + "lastmod").text,
				   'url': test_link
				   }

			if len(self.saved_data) != 0:
				rows_dict = list(self.saved_data.values())
				head_list = [i['headline'] for i in rows_dict]
				if row['headline'] in head_list:  # change here
					j += 1
					if j % 10 == 0:
						time.sleep(1)
					continue

			self.rowlist[test_link] = row

			j += 1
			if j % 10 == 0:
				time.sleep(1)

			i += 1
			if i == self.article_count:
				break

		t10 = time.time()
		avg_time_per_step = round((t10 - t0) / i, 4)
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
