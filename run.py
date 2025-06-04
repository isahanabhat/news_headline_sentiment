import argparse
import os

from news_headline_sentiment import news_scrape
from news_headline_sentiment import news_scrape_bloomberg


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

parser.add_argument('-ds', type=str, help='Start date of crawl')
parser.add_argument('-ws', type=str, help='News website code to crawl')
parser.add_argument('-dl', type=int, help='Download N number of articles')
args = parser.parse_args()

# scrape_obj = news_scrape.NewsScraper(filename, ap_sitemap, 'apnews', "2024-01-01", 450)

file = r"news_data.csv"
DATA_PATH = r"C:\Users\bhats\SAHANABHAT\projects_data"
filename = os.path.join(DATA_PATH, file)
ap_sitemap = r"https://apnews.com/ap-sitemap.xml"


if args.ws is None:
    print("-ws mandatory")
    parser.print_help()
    exit()

scrape_obj = news_scrape.NewsScraper(args.ws)
bb_obj = news_scrape_bloomberg.NewsScrapeBloomberg()

if args.ds:
    if args.ws == 'apnews':
        scrape_obj.url_getall(args.ds)
    elif args.ws == 'bb':
        bb_obj.url_getall(args.ds)
elif args.dl:
    if args.ws == 'apnews':
        scrape_obj.download_headlines(args.dl)
    elif args.ws == 'bb':
        bb_obj.download_headlines(args.dl)
