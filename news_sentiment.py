from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import pandas as pd
import os
import matplotlib.pyplot as plt

nltk.download('vader_lexicon')

analyzer = SentimentIntensityAnalyzer()

file_codes = ['apnews', 'bb', 'cnbc']
data_list = []
for code in file_codes:
    file = "headlines_data_" + code + ".csv"
    # file = r"news_data.csv"
    DATA_PATH = os.getenv('DATA_HOME')
    filepath = os.path.join(DATA_PATH, file)
    test_data = pd.read_csv(filepath)

    test_data = test_data.dropna(subset=['headline'])
    test_data_groups = test_data.groupby('last_modified')
    gb = test_data_groups.groups
    headlines = {}
    for key, value in gb.items():
        h = lambda h_list: ". ".join(h_list)
        headlines[key] = h(list(test_data.loc[value]['headline']))
    data_list.append(headlines)
    # test_data.to_csv('test_file/test.csv')

for i in data_list:
    print(i.keys())


result = {'date': [], 'neg': [], 'pos': [], 'compound': []}
date = 'dt-2025-06-11'
sentence = ''
for i in data_list:
    sentence += i[date]
sentiment_score = analyzer.polarity_scores(sentence)
print(sentiment_score)
result['date'].append(key)
result['pos'].append(sentiment_score['pos'])
result['neg'].append(sentiment_score['neg'])
result['compound'].append(sentiment_score['compound'])

"""
for key in headlines.keys():
    sentence = headlines[key]
    sentiment_score = analyzer.polarity_scores(sentence)
    # print(key, ": ", sentiment_score)
    result['date'].append(key)
    result['pos'].append(sentiment_score['pos'])
    result['neg'].append(sentiment_score['neg'])
    result['compound'].append(sentiment_score['compound'])

sentiment_df = pd.DataFrame.from_dict(result)
sentiment_df['date'] = pd.to_datetime(sentiment_df['date'], dayfirst=True)
sentiment_df = sentiment_df.sort_values(by='date')

print(sentiment_df['date'])
sentiment_file = r"sentiment_score.csv"
sentiment_filepath = os.path.join(DATA_PATH, sentiment_file)
sentiment_df.to_csv(sentiment_filepath, index=False)

sentiment_df.plot(x='date', y='compound')
plt.show()
"""