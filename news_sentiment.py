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
    spacy.cli.download("en_core_web_sm")
    nlp = spacy.load('en_core_web_sm')
    nlp.add_pipe('spacytextblob')
    result = {'date': [], 'polarity_spacy': [], 'subjectivity_spacy': []}
    for key, value in sentence_list.items():
        doc = nlp(value)
        result['date'].append(key.split('dt-')[1])
        result['polarity_spacy'].append(doc._.blob.polarity)
        result['subjectivity_spacy'].append(doc._.blob.subjectivity )
    return result

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
        headlines[key] = h(list(test_data.loc[value]['headline'])) + ". "
    data_list.append(headlines)

dates = list(data_list[0].keys())

sentence_list = {}
for i in dates:
    sentence = ''
    sentence += data_list[0][i]
    sentence += data_list[1][i]
    sentence += data_list[2][i]
    sentence_list[i] = sentence

result_vader = get_sentiment_vader(sentence_list)
result_textblob = get_sentiment_textblob(sentence_list)
result_spacy = get_sentiment_spacy(sentence_list)
print(result_spacy)

vader_df = pd.DataFrame.from_dict(result_vader)
textblob_df = pd.DataFrame.from_dict(result_textblob)
spacy_df = pd.DataFrame.from_dict(result_spacy)

data_frames = [vader_df, textblob_df, spacy_df]

# sentiment_df = vader_df.merge(textblob_df, on='date', how='left')
sentiment_df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), data_frames)
sentiment_df['date'] = pd.to_datetime(sentiment_df['date'], dayfirst=False)
sentiment_df = sentiment_df.sort_values(by='date')

print(sentiment_df['date'])
sentiment_file = r"sentiment_score.csv"
sentiment_filepath = os.path.join(DATA_PATH, sentiment_file)
sentiment_df.to_csv(sentiment_filepath, index=False)

"""sentiment_df.plot(x='date', y='compound_vader')
plt.show()"""
