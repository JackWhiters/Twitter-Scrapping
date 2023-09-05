import snscrape.modules.twitter as sntwitter
import pandas as pd
import streamlit as st
from datetime import date
import pymongo


def tweets_scrap(keyword, start_date, end_date, count):
    result = f"{keyword} since:{start_date} until:{end_date}"
    data = []
    for index, tweet in enumerate(sntwitter.TwitterSearchScraper(result).get_items()):
        if index > count-1:
            break
        data.append([tweet.date,
                     tweet.id,
                     tweet.url,
                     tweet.content,
                     tweet.user.username,
                     tweet.replyCount,
                     tweet.retweetCount,
                     tweet.lang,
                     tweet.source,
                     tweet.likeCount])
    df = pd.DataFrame(data, columns=["Date",
                                     "ID",
                                     "url",
                                     "Content",
                                     "Username",
                                     "Reply count",
                                     "Retweet count",
                                     "Language",
                                     "Source",
                                     "Like count"
                                     ], index=None)
    return df


def upload_to_mongodb(df, keyword):
    today = date.today().strftime("%d-%m-%Y")
    client = pymongo.MongoClient(
        "mongodb+srv://test:<password>@cluster0.btxtni3.mongodb.net/?retryWrites=true&w=majority") #Making connection between atlas mongobd and python
    db = client["Twitter"]
    col = db["Tweets"]
    for index, row in df.iterrows():
        doc = row.to_dict()
        new_doc = {"keyword": keyword, "date": today, "Scrapped data": doc}
        col.insert_one(new_doc)


def display():
    st.title("Twitter Scrapping with Streamlit")
    keyword = st.text_input("Enter the keyword")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Enter From date")
    with col2:
        end_date = st.date_input("Enter To date")
    count = st.number_input("Enter the number of tweets to scrape")

    tweets_scrap(keyword, start_date, end_date, count)

    col_D1, col_D2 = st.columns(2)
    with col_D1:
        csv = df.to_csv()
        st.download_button(label="Download file as csv", data=csv, mime="text/csv")
    with col_D2:
        json = df.to_json()
        st.download_button(label="Download file as json", data=json, mime="application/json")
    if st.button("Upload to MongoDB"):
        upload_to_mongodb(df, keyword)


display()
