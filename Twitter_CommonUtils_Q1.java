package Assignment5.twitter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;
import twitter4j.Status;

public class Twitter_CommonUtils_Q1 implements Serializable
{
	// Universal version identifier for a Serializable class.
	private static final long serialVersionUID = 1L;

	// Prints a sample of the tweets it receives from Twitter every second.
	public JavaDStream<String> getMessages(JavaDStream<Status> tweets)
	{
		return tweets.map(Status::getText);
	}

	// Prints total number of words in each tweet.
	public JavaPairDStream<String, String> getTotalWords(JavaDStream<String> messages)
	{
		return messages.mapToPair(t -> new Tuple2<String, String>("Tweet: " + t,
				" Total Words: " + Arrays.asList(t.split(" ")).size()));
	}

	// Prints total number of characters in each tweet.
	public JavaPairDStream<String, String> getTotalCharacters(JavaDStream<String> messages)
	{
		return messages.mapToPair((String t) -> new Tuple2<String, String>(
				"Tweet: " + t, " Total Characters: " + Arrays.asList(t.split("")).size()));
	}

	// Prints total number of hashtags in each tweet.
	public JavaPairDStream<String, List<String>> getHashtags(JavaDStream<String> messages)
	{
		return messages.mapToPair((String t) -> new Tuple2<String, List<String>>(
				"Tweet: " + t + " Hastags: ",
				// split each tweet with space
				Arrays.asList(t.split(" ")).stream()
				// collect all the words which starts with #
				.filter(x -> x.startsWith("#"))
				.collect(Collectors.toList())));
	}
	
	// variables to store word, character and tweet count
	long countWords = 0L;
	long countCharacters = 0L;
	long countTweets = 0L;

	// Prints average number of words per tweet.
	public void findAvgOfWords(JavaDStream<String> messages)
	{
		messages.foreachRDD(x -> {countTweets += x.count();	});
		// split each tweet by space to get the words
		messages.flatMap(t -> Arrays.asList(t.split(" ")).iterator())
		.foreachRDD(w -> {
			countWords += w.count();
			if (countTweets > 0L)
			{
				System.out.println("Total Words : " + countWords);
				System.out.println("Total Tweets : " + countTweets);
				// print the average word count
				double avgWordsPerTweet = (Double.valueOf(countWords)/ Double.valueOf(countTweets));
				System.out.println("Average number	of words per tweet : " +avgWordsPerTweet );
				countWords = 0L;
				countTweets = 0L;
			}
		});
	}

	// Prints average number of characters per tweet.
	public void findAvgOfCharacters(JavaDStream<String> messages)
	{	
		// get all tweets count
		messages.foreachRDD(x -> {countTweets += x.count(); });
		
		// split each tweet by character to get the words
		messages.flatMap(t -> Arrays.asList(t.split("")).iterator()).foreachRDD(w -> {
			// sum it all
			countCharacters += w.count();
			if (countCharacters > 0L)
			{
				System.out.println("Total Characters : " + countCharacters);
				System.out.println("Total Tweets : " + countTweets);
				// print the average character count
				double avgCharPerTweet = (Double.valueOf(countCharacters)/ Double.valueOf(countTweets));
				System.out.println("Average number of characters per tweet : "+avgCharPerTweet );

				countCharacters = 0L;
				countTweets = 0L;
			}
		});
	}

	// Prints top 10 hashtags from a batch.
	// @params: hashTags = all the hashtags collected from tweet
	// 			n = number of top hashtags to collect
	public void getTopNHashtags(JavaPairDStream<String, List<String>> hashTags,	int n)
	{
		// sort the Hashtags on Value using map reduce approach
		JavaPairDStream<Integer, String> sortedHastags = hashTags
				.map(tuple -> tuple._2()).flatMap(hastag -> hastag.iterator())
				.mapToPair(hastag -> new Tuple2<String, Integer>(hastag, 1))
				// count all the occurance of the hashtag
				.reduceByKey((x, y) -> x + y).mapToPair(tuple -> tuple.swap())
				.transformToPair(tuple -> tuple.sortByKey(false));
		
		// print to 10 hashtags from the sorted hashtag
		sortedHastags.foreachRDD(new VoidFunction<JavaPairRDD<Integer, String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaPairRDD<Integer, String> rdd)
			{
				String out = "\nTop 10 hashtags:\n";
				for (final Tuple2<Integer, String> t : rdd.take(n))
				{
					out = out + t.toString() + "\n";
				}
				System.out.println(out);
			}
		});
	}

	// Repeat computation for the window of length windowSize.
	// @params: n = number of hashtags to consider
	// 			windowSize = duration for which the operation needs to be repeated
	// 			interval = time interval before initiating each set of operation
	public void performWindowedOperations(JavaDStream<String> messages, int n, long windowSize, long interval)
	{
		final JavaDStream<String> windowedMessages = messages.window(new Duration(windowSize), new Duration(interval));

		findAvgOfWords(windowedMessages);
		findAvgOfCharacters(windowedMessages);
		getTopNHashtags(getHashtags(windowedMessages), n);
	}
}
