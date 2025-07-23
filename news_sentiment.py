import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import torch
from transformers import pipeline
from datetime import datetime
import pandas as pd
import os
import matplotlib.pyplot as plt


if __name__ == '__main__':
    file_codes = ['apnews', 'bb', 'cnbc']
    data_list = []
    analyzer = SentimentIntensityAnalyzer()
    sentiment_pipeline = pipeline("sentiment-analysis")

    rows = []
    df = {}
    for code in file_codes:
        file = "headlines_data_" + code + ".csv"
        temp = code + "_temp.csv"
        score = "news_score.csv"

        DATA_PATH = os.getenv('DATA_HOME')
        filepath = os.path.join(DATA_PATH, file)
        temp_path = os.path.join(DATA_PATH, temp)
        score_path = os.path.join(DATA_PATH, score)

        test_data = pd.read_csv(filepath)
        test_data = test_data.dropna(subset=['headline'])
        # test_data = test_data.sample(n=500)
        t1 = time.time()

        # vader
        test_data['sentiment_score'] = test_data.apply(lambda row : analyzer.polarity_scores(row['headline'])['compound'], axis=1)
        test_data['sentiment_type'] = test_data.apply(lambda row : 'pos' if row['sentiment_score'] > 0 else 'neg', axis=1)
        test_data['sentiment_type'] = test_data.apply(lambda row: 0 if row['sentiment_score'] == 0 else row['sentiment_type'], axis=1)

        # hugging face
        test_data['hf_score'] = test_data.apply(lambda row: sentiment_pipeline(row['headline'])[0]['score'], axis=1)
        test_data['hf_type'] = test_data.apply(lambda row: sentiment_pipeline(row['headline'])[0]['label'], axis=1)

        t2 = time.time()
        print('Time Taken = ', t2 - t1)

        test_data.to_csv(temp_path, index=False)
        test_data_groups = test_data.groupby('last_modified')

        news_val = []
        for key, group in test_data_groups:
            x = group.sentiment_type.value_counts()
            if 'pos' not in x:
                news_val.append(0)
            else:
                news_val.append(x['pos']/sum(list(x)))
        df[code] = news_val

        news_val = []
        for key, group in test_data_groups:
            x = group.hf_type.value_counts()
            if 'POSITIVE' not in x:
                news_val.append(0)
            else:
                news_val.append(x['POSITIVE'] / sum(list(x)))
        df[code + "_hf"] = news_val

        print(code, " length = ", len(news_val))
        print()

        if code == 'cnbc':
            days = list(test_data['last_modified'].unique())
            days = [datetime.strptime(i[3:], "%Y-%m-%d").date() for i in days]
            days = sorted(days)
            df['date'] = days

    pos_df = pd.DataFrame(df, columns=['date', 'apnews', 'bb', 'cnbc', 'apnews_hf', 'bb_hf', 'cnbc_hf'])
    pos_df.to_csv(score_path, index=False)

    figure, axs = plt.subplots(2, 1)
    axs[0].plot(pos_df['date'], pos_df['apnews'], label='APNews', color='blue', linestyle='--')
    axs[0].plot(pos_df['date'], pos_df['bb'], label='BloomBerg', color='red', linestyle='--')
    axs[0].plot(pos_df['date'], pos_df['cnbc'], label='CNBC', color='green', linestyle='--')
    axs[0].legend()
    axs[0].set_title('VADER')

    axs[1].plot(pos_df['date'], pos_df['apnews_hf'], label='APNews', color='blue', linestyle='--')
    axs[1].plot(pos_df['date'], pos_df['bb_hf'], label='BloomBerg', color='red', linestyle='--')
    axs[1].plot(pos_df['date'], pos_df['cnbc_hf'], label='CNBC', color='green', linestyle='--')
    axs[1].legend()
    axs[1].set_title('HUGGINGFACE')

    plt.show()
