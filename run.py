import argparse
import os

from news_headline_sentiment import news_scrape
from news_headline_sentiment import news_scrape_bloomberg
from news_headline_sentiment import news_scrape_cnbc
from news_headline_sentiment import news_scrape_apnews

# python C:\Users\bhats\SAHANABHAT\python_projects\news_headline_sentiment\run.py -ws apnews -ds 2025-06-09
# python C:\Users\bhats\SAHANABHAT\python_projects\news_headline_sentiment\run.py -ws apnews -dl 10

# python C:\Users\bhats\SAHANABHAT\python_projects\news_headline_sentiment\run.py -ws bb -ds 2025-03-01
# python C:\Users\bhats\SAHANABHAT\python_projects\news_headline_sentiment\run.py -ws bb -dl 100

# python C:\Users\bhats\SAHANABHAT\python_projects\news_headline_sentiment\run.py -ws cnbc -ds 2025-06-09
# python C:\Users\bhats\SAHANABHAT\python_projects\news_headline_sentiment\run.py -ws bb -dl 10

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='News Headline Sentiment',
        description='Retrieve and store URLs and headline of news articles of various news sites',
        epilog='Goodnight and good luck :)'
    )

    parser.add_argument('-ds', type=str, help='Start date of crawl')
    parser.add_argument('-ws', type=str, help='News website code to crawl')
    parser.add_argument('-dl', type=int, help='Download N number of articles')
    args = parser.parse_args()


    file = r"news_data.csv"
    DATA_PATH = r"C:\Users\bhats\SAHANABHAT\projects_data"
    filename = os.path.join(DATA_PATH, file)
    ap_sitemap = r"https://apnews.com/ap-sitemap.xml"


    if args.ws is None:
        print("-ws mandatory")
        parser.print_help()
        exit()

    if args.ds:
        if args.ws == 'apnews':
            apnews_obj = news_scrape_apnews.NewsScrapeAPNews()
            apnews_obj.url_getall(args.ds)
        elif args.ws == 'bb':
            bb_obj = news_scrape_bloomberg.NewsScrapeBloomberg()
            bb_obj.url_getall(args.ds)
        elif args.ws == 'cnbc':
            cnbc_obj = news_scrape_cnbc.NewsScrapeCNBC()
            cnbc_obj.url_getall(args.ds)
    elif args.dl:
        if args.ws == 'apnews':
            apnews_obj = news_scrape_apnews.NewsScrapeAPNews()
            apnews_obj.download_headlines(args.dl)
        elif args.ws == 'bb':
            bb_obj = news_scrape_bloomberg.NewsScrapeBloomberg()
            bb_obj.download_headlines(args.dl)
        elif args.ws == 'cnbc':
            cnbc_obj = news_scrape_cnbc.NewsScrapeCNBC()
            cnbc_obj.download_headlines(args.dl)
