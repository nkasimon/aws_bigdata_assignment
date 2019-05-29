from pyhive import hive
import sys
import pandas as pd
import matplotlib.pyplot as plt

conn = hive.Connection(host="localhost")
sql_query = "SELECT `user`.screen_name, created_at, source, favorite_count as likes, retweeted_status.retweet_count as retweets, length(text) as tweet_length from tweets"
df = pd.read_sql(sql_query, conn)
#print(sys.getsizeof(df))
#df.head()
#print(df.head(10))


# Time Series of variation of tweets length over time
time_likes = pd.Series(data=df['tweet_length'].values, index=df['created_at'])
time_likes.plot(figsize=(16, 4), color='r')
#plt.show()
plt.savefig('output1.png')

# Time Series of variation of likes over time
time_favs = pd.Series(data=df['likes'].values, index=df['created_at'])
time_favs.plot(figsize=(16, 4), color='r')
#plt.show()
plt.savefig('output2.png')

# Time Series of variation of retweets length over time
time_retweets = pd.Series(data=df['retweets'].values, index=df['created_at'])
time_retweets.plot(figsize=(16, 4), color='r')
#plt.show()
plt.savefig('output3.png')


# Time Series of variation of likes length over time with legend
time_likes = pd.Series(data=df['likes'].values, index=df['created_at'])
time_likes.plot(figsize=(16, 4), label="likes", legend=True)
plt.savefig('output4.png')

# Time Series of variation of retweets length over time with legend
time_retweets = pd.Series(data=df['retweets'].values, index=df['created_at'])
time_retweets.plot(figsize=(16, 4), label="retweets", legend=True)
#plt.show()
plt.savefig('output5.png')



# Plot number of tweets and number of retweets per twitter_account
sql_query = "SELECT t.retweeted_screen_name as retweeted_screen_name, sum(retweets) AS total_retweets, count(*) AS tweet_count FROM (SELECT retweeted_status.`user`.screen_name as retweeted_screen_name, retweeted_status.text, max(retweeted_status.retweet_count) as retweets FROM tweets1 GROUP BY retweeted_status.`user`.screen_name, retweeted_status.text) t GROUP BY t.retweeted_screen_name ORDER BY total_retweets DESC LIMIT 10"
df = pd.read_sql(sql_query, conn)

ax = plt.gca()
df.plot(kind='line',x='retweeted_screen_name',y='total_retweets',ax=ax)
df.plot(kind='line',x='retweeted_screen_name',y='tweet_count', color='red', ax=ax)
plt.savefig('output6.png')


                           
