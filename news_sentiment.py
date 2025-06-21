import time
from operator import index

from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
from textblob import TextBlob
import spacy
import spacy.cli
from spacytextblob.spacytextblob import SpacyTextBlob

import pandas as pd
from functools import reduce
import os
import matplotlib.pyplot as plt
from textblob.en import sentiment


def get_sentiment_vader(sentence_list):
    # nltk.download('vader_lexicon')
    analyzer = SentimentIntensityAnalyzer()
    result = {'date': [], 'pos_vader': [], 'neu_vader': [], 'neg_vader': [], 'compound_vader': []}
    for key, value in sentence_list.items():
        sentiment_score = analyzer.polarity_scores(value)
        result['date'].append(key.split('dt-')[1])
        result['pos_vader'].append(sentiment_score['pos'])
        result['neu_vader'].append(sentiment_score['neu'])
        result['neg_vader'].append(sentiment_score['neg'])
        result['compound_vader'].append(sentiment_score['compound'])
    return result

def get_sentiment_textblob(sentence_list):
    result = {'date': [], 'polarity_tb': [], 'subjectivity_tb': []}
    for key, value in sentence_list.items():
        blob = TextBlob(value)
        sentiment_score = blob.sentiment
        result['date'].append(key.split('dt-')[1])
        result['polarity_tb'].append(sentiment_score.polarity)
        result['subjectivity_tb'].append(sentiment_score.subjectivity)
    return result

def get_sentiment_spacy(sentence_list):
    # spacy.cli.download("en_core_web_sm")
    nlp = spacy.load('en_core_web_sm')
    nlp.add_pipe('spacytextblob')
    result = {'date': [], 'polarity_spacy': [], 'subjectivity_spacy': []}
    for key, value in sentence_list.items():
        doc = nlp(value)
        result['date'].append(key.split('dt-')[1])
        result['polarity_spacy'].append(doc._.blob.polarity)
        result['subjectivity_spacy'].append(doc._.blob.subjectivity )
    return result

file_codes = ['apnews', 'bb']
data_list = []
analyzer = SentimentIntensityAnalyzer()
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
    # print("1",len(test_data['last_modified'].unique()))
    test_data = test_data.dropna(subset=['headline'])
    # print("2",len(test_data['last_modified'].unique()))

    t1 = time.time()
    test_data['sentiment_score'] = test_data.apply(lambda row : analyzer.polarity_scores(row['headline'])['compound'], axis=1)
    test_data['sentiment_type'] = test_data.apply(lambda row : 'pos' if row['sentiment_score'] > 0 else 'neg', axis=1)
    test_data['sentiment_type'] = test_data.apply(lambda row: 0 if row['sentiment_score'] == 0 else row['sentiment_type'], axis=1)
    print(time.time() - t1)
    test_data.to_csv(temp_path)
    test_data_groups = test_data.groupby('last_modified')

    news_val = []
    for key, group in test_data_groups:
        x = group.sentiment_type.value_counts()
        news_val.append(x['pos']/sum(list(x)))
    df[code] = news_val

    if code == 'bb':
        days = list(test_data['last_modified'].unique())
        days.reverse()
        df['date'] = days

# print(df)
pos_df = pd.DataFrame(df, columns=['date', 'apnews', 'bb'])
pos_df.to_csv(score_path, index=False)

figure, axs = plt.subplots(2, 1)

axs[0].plot(pos_df['date'], pos_df['apnews'], label='Compound', color='black', linestyle='--')
axs[0].set_title('AP News')

axs[1].plot(pos_df['date'], pos_df['bb'], label='Compound', color='black', linestyle='--')
axs[1].set_title('BloomBerg')

plt.show()
# gb = test_data_groups.groups
"""
    gb = test_data_groups.groups
    headlines = {}
    for key, value in gb.items():
        h = lambda h_list: ". ".join(h_list)
        headlines[key] = h(list(test_data.loc[value]['headline'])) + ". "
    data_list.append(headlines)

dates = list(data_list[0].keys())


sentence_list = {}
for i in dates:
    try:
        sentence = ''
        sentence += data_list[0][i]
        sentence += data_list[1][i]
        sentence += data_list[2][i]
    except Exception as e:
        print(str(e))
    sentence_list[i] = sentence

result_vader = get_sentiment_vader(sentence_list)
result_textblob = get_sentiment_textblob(sentence_list)
result_spacy = get_sentiment_spacy(sentence_list)

vader_df = pd.DataFrame.from_dict(result_vader)
textblob_df = pd.DataFrame.from_dict(result_textblob)
spacy_df = pd.DataFrame.from_dict(result_spacy)

data_frames = [vader_df, textblob_df, spacy_df]

# sentiment_df = vader_df.merge(textblob_df, on='date', how='left')
sentiment_df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), data_frames)
sentiment_df['date'] = pd.to_datetime(sentiment_df['date'], dayfirst=False)
sentiment_df = sentiment_df.sort_values(by='date')

# print(sentiment_df['date'])
sentiment_file = r"sentiment_score.csv"
sentiment_filepath = os.path.join(DATA_PATH, sentiment_file)
sentiment_df.to_csv(sentiment_filepath, index=False)
sent_columns = sentiment_df.columns
# sentiment_df.plot(x='date', y='compound_vader')
# plt.show()
print(sent_columns)

figure, axs = plt.subplots(3, 1)
axs[0].plot(sentiment_df['date'], sentiment_df['pos_vader'], label='Positive', color='green')
axs[0].plot(sentiment_df['date'], sentiment_df['neu_vader'], label='Neutral', color='blue')
axs[0].plot(sentiment_df['date'], sentiment_df['neg_vader'], label='Negative', color='red')
axs[0].plot(sentiment_df['date'], sentiment_df['compound_vader'], label='Compound', color='black', linestyle='--')
axs[0].set_title('VADER')
axs[0].legend()

axs[1].plot(sentiment_df['date'], sentiment_df['polarity_tb'], label='Polarity', color='purple')
axs[1].plot(sentiment_df['date'], sentiment_df['subjectivity_tb'], label='Subjectivity', color='orange')
axs[1].set_title('TextBlob')
axs[1].legend()

axs[2].plot(sentiment_df['date'], sentiment_df['polarity_spacy'], label='Polarity', color='brown')
axs[2].plot(sentiment_df['date'], sentiment_df['subjectivity_spacy'], label='Subjectivity', color='cyan')
axs[2].set_title('spaCY')
axs[2].legend()

# sentiment_df.plot(title='sentiment', x='date', y=sent_columns[1:])
plt.show()"""