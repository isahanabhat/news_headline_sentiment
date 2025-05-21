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


def read_file_data(filepath):
	saved_data = {}
	if os.path.exists(filepath):
		file_data = pd.read_csv(filepath)
		file_data["url_index"] = file_data["url"]
		saved_data = file_data.set_index('url_index').to_dict('index')
	return saved_data

def site_data_retriever(sitemap_url):
	HEADERS = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
	}
	data = requests.get(sitemap_url, headers=HEADERS)
	input = (data.content).decode("utf-8")
	data_tree = ET.ElementTree(ET.fromstring(input))
	root = data_tree.getroot()
	return root

def site_crawler(news_code, start_date, to_date, data_frame, sitemap_root, verbose=False):
	rowlist = {}
	session = requests.session()

	# root = site_data_retriever(sitemap_url)
	HEADERS = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
	}
	i = 0
	j = 0
	t0 = time.time()

	for child in sitemap_root:
		tags = re.split(r'url', child.tag)
		element = tags[0]+"loc"
		t1 = time.time()

		test_link = child.find(element).text
		t2 = time.time()

		test_link_data = session.get(test_link, headers=HEADERS)
		t3 = time.time()

		soup_data = BeautifulSoup(test_link_data.content, 'html.parser')
		t4 = time.time()

		if verbose:
			print("i = ", i)
			print("step1 ", t2 - t1)
			print("step2 ", t3 - t2)
			print("step3 ", t4 - t3)

		# row change here
		row = {'code': news_code,
			  'headline': soup_data.find('title').text,
		   # 'date_published': soup_data.find('template', attr={'data-date-tpl': True}),
			  'last_extracted': datetime.today().strftime('%d-%m-%Y %H:%M:%S'),
			  'last_modified': child.find(tags[0] + "lastmod").text,
			  'url': test_link
		}

		if len(data_frame) != 0:
			rows_dict = list(data_frame.values())
			head_list = [i['headline'] for i in rows_dict]
			if row['headline'] in head_list: # change here
				j += 1
				if j % 10 == 0:
					time.sleep(1)
				continue

		rowlist[test_link] = row

		j += 1
		if j % 10 == 0:
			time.sleep(1)

		i += 1
		if i == 10:
			break

	t10 = time.time()
	avg_time_per_step = round((t10-t0)/i, 4)
	# print("avg time = ", avg_time_per_step)
	return rowlist

def save_to_file(rows_to_add, rows_existing, filename):
	new_rows = pd.DataFrame(rows_to_add.values())
	news_data = pd.concat([pd.DataFrame(rows_existing.values()), new_rows], ignore_index=True)

	# print(news_data)
	# print("time more than 1 = ", long_time)
	print(news_data)
	news_data.to_csv(filename, index=False)

file = r"news_data.csv"
DATA_PATH = r"C:\Users\bhats\SAHANABHAT\projects_data"
filename = os.path.join(DATA_PATH, file)

ap_2025_03 = r'https://apnews.com/ap-sitemap-202503.xml'
re_2025_05_18 = r'https://www.reuters.com/arc/outboundfeeds/sitemap2/2025-05-18/?outputType=xml'  # doesnt work
wsj_2025_05 = r'https://www.wsj.com/sitemaps/web/wsj/en/sitemap_wsj_en_m5_2025.xml' 			  # doesnt work
cnbc_url = r'https://www.cnbc.com/CNBCsitemapAll12.xml'

codes = ['apnews', 'reuters', 'wsj', 'cnbc']

file_data = read_file_data(filename) # read existing data
sitemap_data = site_data_retriever(ap_2025_03) # scrape news sitemap data
new_news_data = site_crawler(codes[0], 0, 0, file_data, sitemap_data, True) # get news data that isnt in news file
save_to_file(new_news_data, file_data, filename) # save data