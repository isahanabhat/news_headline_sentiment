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

file = r"news_data.csv"
DATA_PATH = r"C:\Users\bhats\SAHANABHAT\projects_data"

filename = os.path.join(DATA_PATH, file)

saved_data = {}
if os.path.exists(filename):
	file_data = pd.read_csv(filename)
	file_data["url_index"] = file_data["url"]
	saved_data = file_data.set_index('url_index').to_dict('index')

rowlist = {}

ap_2025_03 = r'https://apnews.com/ap-sitemap-202503.xml'
session = requests.session()

data = requests.get(ap_2025_03)
input = (data.content).decode("utf-8")
data_tree = ET.ElementTree(ET.fromstring(input))
root = data_tree.getroot()

i = 0
j = 0
long_time = []

t0 = time.time()

for child in root:
	tags = re.split(r'url', child.tag)
	element = tags[0]+"loc"
	t1 = time.time()
	print("i = ", i)
	test_link = child.find(element).text

	t2 = time.time()
	print("step1 ", t2-t1)

	test_link_data = session.get(test_link)
	t3 = time.time()
	print("step2 ", t3-t2)

	soup_data = BeautifulSoup(test_link_data.content, 'html.parser')
	t4 = time.time()
	print("step3 ", t4-t3)

	row = {'code':'apnews',
		   'headline':soup_data.find('title').text,
		   'date_published': soup_data.find('template', attr={'data-date-tpl':True}),
		   'last_extracted': datetime.today().strftime('%d-%m-%Y %H:%M:%S'),
		   'last_modified': child.find(tags[0]+"lastmod").text,
		   'url': test_link
	}

	if len(saved_data) != 0:
		rows_dict = list(saved_data.values())
		head_list = [i['headline'] for i in rows_dict]
		if row['headline'] in head_list:
			j += 1
			if j % 10 == 0:
				time.sleep(1)
			continue

	rowlist[test_link] = row

	if t3-t2 > 1 or t4-t3 > 1:
		long_time.append(i)
	j += 1
	if j % 10 == 0:
		time.sleep(1)

	i += 1
	if i == 10:
		break

t10 = time.time()
avg_time_per_step = round((t10-t0)/i, 4)
# print("avg time = ", avg_time_per_step)

new_rows = pd.DataFrame(rowlist.values())
news_data = pd.concat([pd.DataFrame(saved_data.values()), new_rows], ignore_index=True)

# print(news_data)
# print("time more than 1 = ", long_time)

news_data.to_csv(filename, index=False)
