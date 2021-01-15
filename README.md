# Real-Time-Twitter-Steaming
Spark streaming program that connects to twitter and prints a sample of the tweets it receives from twitter every second. Twitter authentication tokens used for processing Twitter’s real time sample stream. 

Implemented functions to count the number of characters, words and extract the hashtags in each tweet. All code through Spark RDD streams

Extended this further to calcualte:
1) the average number of characters and words per tweet 
2) count the top 10 hashtags
3) for the last 5 minutes of tweets, continuously repeat the computation every 30 seconds.
