package com.sundogsoftware.spark.core

import org.apache.spark.{SparkContext, SparkConf}
import com.sundogsoftware.spark.utils._
import com.sundogsoftware.spark.utils.TweetUtils.Tweet
import org.apache.log4j._
import org.apache.spark.rdd._

import scala.collection.Map

object Ex4InvertedIndex {

  /**
   *
   *  Buildind a hashtag search engine
   *
   *  The goal is to build an inverted index. An inverted is the data structure used to build search engines.
   *
   *  How does it work?
   *
   *  Assuming #spark is an hashtag that appears in tweet1, tweet3, tweet39.
   *  The inverted index that you must return should be a Map (or HashMap) that contains a (key, value) pair as (#spark, List(tweet1,tweet3, tweet39)).
   *
   *  Use the Ex4InvertedIndexSpec to implement the code.
   */
  def invertedIndex(): Array[(String, Int)] = {
    // create spark  configuration and spark context
    val conf = new SparkConf ()
        .setAppName ("Inverted index")
        .setMaster ("local[*]")

    val sc = new SparkContext (conf)

    val tweets = sc.textFile ("data/reduced-tweets.json")
        .mapPartitions (TweetUtils.parseFromJson (_) )

    // Let's try it out!
    // Hint:
    // For each tweet, extract all the hashtag and then create couples (hashtag,tweet)
    // Then group the tweets by hashtag
    // Finally return the inverted index as a map structure
    // TODO write code here
    val text = tweets.map(x => x.text)
    
    val reg = """(#\w+)""".r
    

    val wordTweet = text.flatMap(tuple => tuple.split(" ").map(str => (str, tuple)).toList)
    
    val hashTweets = wordTweet.filter(x => reg.pattern.matcher(x._1).matches)
    
    val groupedHash = hashTweets.groupByKey()
    
    val hashCounts = groupedHash.map(x => (x._1, x._2.size)).sortBy(x => x._2, false).take(10)
    (hashCounts)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val results = invertedIndex
    val answer = results
    answer.foreach(println) 

}
  
  
}
    
    
