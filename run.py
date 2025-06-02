import argparse
import os

from news_headline_sentiment import news_scrape

"""
parser = argparse.ArgumentParser(
                    prog='ProgramName',
                    description='What the program does',
                    epilog='Text at the bottom of help')
"""

parser = argparse.ArgumentParser(
	prog='News Headline Sentiment',
	description='Retrieve and store URLs and headline of news articles of various news sites',
	epilog='Goodnight and good luck :)'
)

parser.add_argument('-ds', '--datestore', type=str, help='Start date of crawl', action='store')
parser.add_argument('-ws', '--website', type=str, help='News website code to crawl', action='store')

args = parser.parse_args()

# scrape_obj = news_scrape.NewsScraper(filename, ap_sitemap, 'apnews', "2024-01-01", 450)

file = r"news_data.csv"
DATA_PATH = r"C:\Users\bhats\SAHANABHAT\projects_data"
filename = os.path.join(DATA_PATH, file)
ap_sitemap = r"https://apnews.com/ap-sitemap.xml"

scrape_obj = news_scrape.NewsScraper(filename, ap_sitemap, args.website, args.datestore, 450)
scrape_obj.__url_get__()