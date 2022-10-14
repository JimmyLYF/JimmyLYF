# -*- coding: utf-8 -*-
"""
@author: SCYS057
"""
import flask
from flask import Flask, request, redirect, url_for, flash, jsonify
import numpy as np
import pickle as p
import json
import re
from flasgger import Swagger
import string
import nltk
from nltk.stem import WordNetLemmatizer 
import pandas as pd


stop_words = p.load(open('stop_words.ob', 'rb'))


def text_cleaning(txt):
    
    text = txt.lower()
    
    text = re.sub(r'[a-z]{1}[0-9]{9}', '', text)
    text = re.sub(r'[a-z]{1}[0-9]{7}[a-z]{1}', 'UniNRIC', text)
    text = re.sub(r'[0-9]{8}', '', text)
    
    text = " ".join([word for word in nltk.word_tokenize(text) if word not in stop_words])
    text = " ".join(word.strip(string.punctuation) for word in text.split())

    emoji_pattern = re.compile("["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text) # Remove Emoji

    lemmatizer = WordNetLemmatizer()
    text = ' '.join([lemmatizer.lemmatize(word) for word in nltk.word_tokenize(text)])
    
    return text

def data_preprocessing(js):

#     jdata = json.loads(js)
    df = pd.DataFrame(js, columns = ['Date', 'Text', 'Session'])
    df['text_clean'] = df['Text'].apply(lambda x:text_cleaning(x))

    df = df.groupby(['Session'])['text_clean'].apply(lambda x: ','.join(x)).reset_index()
    
    df['WordsAsked'] = df['text_clean'].str.split().str.len()
    
    return df[['Session', 'WordsAsked']]


app = Flask(__name__)
Swagger(app)


@app.route('/AbuseDetection/', methods=['POST'])
def predict():
    """ Endpoint taking one input
    ---
    parameters:
        - name: Date
          in: query
          type: datetime
          required: false
        - name: Text
          in: query
          type: string
          required: true
        - name: Session
          in: query
          type: string
          required: true
    responses:
        200:
            description: "0: Non-Abuse, 1: Abuse"
    """

    Date = flask.request.args.get("Date")
    Text = flask.request.args.get("Text")
    Session = flask.request.args.get("Session")

    input_data = np.array([[Date, Text, Session]])
    
    features = data_preprocessing(input_data)
    
    modelfile = 'flask_abuse_detection.sav'
    model = p.load(open(modelfile, 'rb'))
    
    prediction = model.predict(features[['WordsAsked']])

    return str(prediction[0])

@app.route('/', methods=['GET'])
def index():
    return 'Abuse Detection Inference'

if __name__ == '__main__':
#    modelfile = 'flask_abuse_detection.sav'
#    model = p.load(open(modelfile, 'rb'))
    app.run(debug=True, host='0.0.0.0')

