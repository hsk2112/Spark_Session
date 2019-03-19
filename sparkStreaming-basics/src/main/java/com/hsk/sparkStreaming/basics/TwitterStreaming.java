package com.hsk.sparkStreaming.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import com.google.common.collect.Iterables;

/**
 * @author Amit Kumar
 *
 * Demonstrates Apache Spark Streaming functioning by consuming data from Twitter and printing number of tweets
 *  written by an user in a fixed period (10 seconds in our case).
 */
public class TwitterStreaming {
	public static void main(String[] args) {
		// Prepare the spark configuration by setting application name and master node "local" i.e. embedded mode
		final SparkConf sparkConf = new SparkConf().setAppName("Twitter Data Processing").setMaster("local[10]");
		// Create Streaming context using spark configuration and duration for which messages will be batched and fed to Spark Core
		final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Duration.apply(10000));
		
		// Prepare configuration for Twitter authentication and authorization
		final Configuration conf = new ConfigurationBuilder().setDebugEnabled(false)
										.setOAuthConsumerKey("MxQCTyFaITn01Jde4SLof7ZfR")
										.setOAuthConsumerSecret("4eL6g2Iv2dJgei0iBIaowMsEfbG1q14dhwimE3kpBT9VcYGoAG")
										.setOAuthAccessToken("3457451234-rv4lhFAYg0t20pq0pswXLR1G0mtUKjZlmv7jH4x")
										.setOAuthAccessTokenSecret("o3yz45UqwxclV0mUwLM8XQveQEh0yYFATZBrQnag0z1Zx")
										.build();
		// Create Twitter authorization object by passing prepared configuration containing consumer and access keys and tokens
		final Authorization twitterAuth = new OAuthAuthorization(conf);
		// Create a data stream using streaming context and Twitter authorization
		final JavaReceiverInputDStream<Status> inputDStream = TwitterUtils.createStream(streamingContext, twitterAuth, new String[]{});
		/* JavaDStream<String> statuses = inputDStream.map(
	                new Function<Status, String>() {
	                    public String call(Status status) { return status.getText(); }
	                }
	        ); */
		 
		// Create a new stream by filtering the non english tweets from earlier streams
		final JavaDStream<Status> enTweetsDStream = inputDStream.filter((status) -> "en".equalsIgnoreCase(status.getLang()));
		// Convert stream to pair stream with key as user screen name and value as tweet text
		final JavaPairDStream<String, String> userTweetsStream = 
								enTweetsDStream.mapToPair(
									(status) -> new Tuple2<String, String>(status.getUser().getScreenName(), status.getText())
								);
		/*
		// Group the tweets for each user
		final JavaPairDStream<String, Iterable<String>> tweetsReducedByUser = userTweetsStream.groupByKey();
		// Create a new pair stream by replacing iterable of tweets in older pair stream to number of tweets
		final JavaPairDStream<String, Integer> tweetsMappedByUser = tweetsReducedByUser.mapToPair(
					userTweets -> new Tuple2<String, Integer>(userTweets._1, Iterables.size(userTweets._2))
				);
		// Iterate over the stream's RDDs and print each element on console
		tweetsMappedByUser.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>)pairRDD -> {
			pairRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {

				@Override
				public void call(Tuple2<String, Integer> t) throws Exception {
					System.out.println(t._1() + "," + t._2());
				}
				
			});
		});*/
		userTweetsStream.print();
		// Triggers the start of processing. Nothing happens if streaming context is not started
		streamingContext.start();
		// Keeps the processing live by halting here unless terminated manually
			/* Sleep for some seconds. */
		streamingContext.awaitTermination();
		
		
		
	}
}