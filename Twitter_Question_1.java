package Assignment5.twitter;

import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class Twitter_Question_1 
{
	public static void main(String[] args) throws Exception 
	{
		// Turn off INFO Logging by Spark.
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// Setting configuration for Spark to use 4 cores of processor and 4 Giga bytes of memory
		SparkConf sparkConf = new SparkConf()
				.setAppName("Twitter_Assignment_5")
				.setMaster("local[4]").set("spark.executor.memory", "4g");

		// Set the system properties so that Twitter4j library used by Twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", "X0VySedMcuCkiaVtSLPUjK5mj");
		System.setProperty("twitter4j.oauth.consumerSecret", "cviHh4K4An2nmixjQouH1Z8gDa5Qi6wZmfinkmiwW3NUFUyKzu");
		System.setProperty("twitter4j.oauth.accessToken", "2893719320-nMSpXFKc9y6EaZTG0M7OZVGATRp1Fm2I9U8hHvL");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "IwYqun3iHVulLpY2jW3R7VOtEoJuAAIx0Q74Iq34VO0tD");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		JavaDStream<Status> tweets = TwitterUtils.createStream(ssc);
		
		// instantiate Twitter_CommonUtils_Q1 class
		// this class contains all the functions reuired to accomplish Question 1
		Twitter_CommonUtils_Q1 service = new Twitter_CommonUtils_Q1();


		// Q1 Prints a sample (10 messages) of the tweets it receives from twitter every second.
		JavaDStream<String> messages = service.getMessages(tweets);
		messages.print();

		// Q2 Prints total number of words in each tweet.
		JavaPairDStream<String, String> words = service.getTotalWords(messages);
		words.print();

		// Q2 Prints total number of characters in each tweet.
		JavaPairDStream<String, String> characters = service.getTotalCharacters(messages);
		characters.print();

		// Q2 Prints total number of hashtags in each tweet.
		JavaPairDStream<String, List<String>> hashtags = service.getHashtags(messages);
		hashtags.print();

		// Q3(a) Prints average number of words per tweet.
		service.findAvgOfWords(messages);

		// Q3(a) Prints average number of characters per tweet.
		service.findAvgOfCharacters(messages);

		// Q3(b) Prints top 10 hashtags
		int noOfHastags = 10;
		service.getTopNHashtags(hashtags, noOfHastags);

		// Q3(c) Repeat computation for the last 5 minutes of tweets every 30sec.
		int interval = 30 * 1000;
		int windowSize = 5 * 60 * 1000;
		service.performWindowedOperations(messages, noOfHastags, windowSize, interval);

		// starting java streaming context
		ssc.start();
		ssc.awaitTermination();	
	}

}
