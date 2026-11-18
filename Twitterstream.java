//Ronan Murphy 15397831
//Assignment 5 Part A

package assignment5;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;

import scala.Tuple2;
import twitter4j.Status;

public class Twitterstream {
    public static void main(String[] args) throws InterruptedException {    	
    	//Part 1: Setup the Spark Streaming Program that prints a sample of Tweets
    	
    	
    	//set consumerkey and consumer secret along with access token and secret access token
    	//these are used to connect to my twitter stream where I can access tweets
    	final String consumerKey = "";// enter private key here
        final String consumerSecret = "";// enter private key here
        final String accessToken = "";// enter private key here
        final String accessTokenSecret = "";// enter private key here

      
        //Spark configureation file using 2 local disks and named Tweet Streamer
        SparkConf sc = new SparkConf().setMaster("local[2]").setAppName("Tweet Streamer");
        //the java streaming context is created to read tweets every 1000 milliseconds(1 second)
        JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(1000));
        //the properties of the keys are setup here and assigned to the oauthorisation twitter4j import
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
        
        //java d stream is created using the context already setup
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jsc);
        
        //Logger reports containing "org" and "akka" are removed to give a clean console output 
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //this reads in stream and get the text of the tweets
        JavaDStream<String> stream = twitterStream.map(status -> status.getText());
        
        //returns a sample of tweets every second
        stream.print();
        
        //Part 2 - Return the count of characters, words and the hashtags in tweets
        
        
        //this java stream iterates through the twitter stream and maps it to a list splitting it by a space
        //this list contains all the words in each tweet
        JavaDStream<String> words = stream.flatMap(status -> Arrays.asList(status.split(" ")).iterator());
        
        //this function returns the character count of each tweet by print the length
        JavaDStream<Integer> charcount = stream.map(status -> status.length());
        charcount.print();
        
        //this function returns the word count of each tweet using the stream of words and returning the length
        JavaDStream<Integer> wordcount = words.map(status -> status.length());
        wordcount.print();
       
        //this function returns every word that starts with a hashtag, the words stream is filtered to do this
        JavaDStream<String> hashTags = words.filter( word -> word.startsWith("#"));
        hashTags.print();
        
        
        //Part 3 - Return the average word and character count, return the top 10 hashtags and repeat these tasks
        // in a window repeating every 30 seconds over the last 5 minutes
        
        
        //for the stream and the word return the average length of characters and words in tweets
        // the character counts are added together and divided by the total amount of tweets
        // this will return the average length of character length and word length
        //an error appear in a special case at the beginning where length was zero which resulted in null output
        // therefore the code only runs when the count isn't 0
        //initally planned on using mean() function but didnt add the counts because the array was empty, I used 
        //this method over the window as it was never empty after 30 seconds
       charcount.foreachRDD(ch ->  {
    	    long countTweet = ch.count() == 0 ? 1 : ch.count();
        	long cha = ch.fold(0, Integer::sum);
        	System.out.println("Average Character Length: " + cha/countTweet);
        });
        
        wordcount.foreachRDD(word ->  {
        	long wTweets = word.count() == 0 ? 1 :word.count();
         	long w = word.fold(0, Integer::sum);
        	System.out.println("Average Word Length: " + w/wTweets);
        });

        
	
       //this is the function to map the hashtags with a key of the count of the hashtag
        // this creates a key value pair which can be reduced
        JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(hash -> new Tuple2<String, Integer>(hash, 1));
        
        //this is used to count by the recurrences of the hashtags 
        //the hashtags are reduced by key and sorted in descending order and the top 10 most popular are printed
        hashTagCount.reduceByKey((a, b) -> a + b).foreachRDD(sort -> sort.mapToPair(hash -> hash.swap())
        		.sortByKey(false).take(10).forEach(print -> System.out.println(print)));
        
        
        //the same method is used to calculate the average length of characters and words
        // this time the window of 5 minutes is defined and the average value is returned every 30 seconds
        stream.window(Durations.minutes(5), Durations.seconds(30))
        .foreachRDD(ch->  {
        	Double chavg = ch.map(s -> s.split("")).mapToDouble(avg -> avg.length).mean();
        	System.out.println("Average Characters: " + chavg);
        });
        
        stream.window(Durations.minutes(5), Durations.seconds(30)).foreachRDD(word ->  {
        	Double wavg = word.map(s -> s.split(" ")).mapToDouble(avg -> avg.length).mean();
        	System.out.println("Average Word: " + wavg);
        });
        
        

        //this is slightly different as I was able to use the mean() function while using a window
        //without a window the rdd for word and character count average was empty
        //this time is is reduced by key 
        //and by the window which returns the top 10 hashtags over the last 5 minutes every 30 seconds
        hashTagCount.reduceByKeyAndWindow((a, b) -> a + b, Durations.minutes(5), Durations.seconds(30))
        .foreachRDD(sort -> sort.mapToPair(hash -> hash.swap()).sortByKey(false).take(10)
						.forEach(print -> System.out.println(print)));
      
        	    	    

        //now we start the the context and the setup keeps running until requested to terminate
        jsc.start();
        jsc.awaitTermination();
    }
}

