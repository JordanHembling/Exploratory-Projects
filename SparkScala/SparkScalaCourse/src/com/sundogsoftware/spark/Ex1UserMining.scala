package com.sundogsoftware.spark.core


import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.rdd._
import com.sundogsoftware.spark.utils.TweetUtils
import com.sundogsoftware.spark.utils.TweetUtils._
import org.apache.log4j._

/**
 *  The scala API documentation: http://spark.apache.org/docs/latest/api/scala/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. The data are reduced tweets as the example below:
 *
 *  {"id":"572692378957430785",
 *  "user":"Srkian_nishu :)",
 *  "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *  "place":"Orissa",
 *  "country":"India"}
 *
 *  We want to make some computations on the users:
 *  - find all the tweets by user
 *  - find how many tweets each user has
 *
 *  Use the Ex1UserMiningSpec to implement the code.
 */
object Ex1UserMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  def loadData(): RDD[Tweet] = {
    // Create the spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("User mining")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile).mapPartitions(TweetUtils.parseFromJson(_))
  }

  /**
   *   For each user return all his tweets
   */
  def tweetsByUser(): RDD[(String, Iterable[Tweet])] = {
    val tweets = loadData
    // TODO write code here
    // Hint: the Spark API provides a groupBy method
    val userInfo = tweets.groupBy(x => x.user)
    (userInfo)
  }

  /**
   *  Compute the number of tweets by user
   */
  def tweetByUserNumber(): RDD[(String, Int)] = {
    // TODO write code here
    // Hint: think about what you did in the wordcount example
    val userInfo = tweetsByUser
    val userTweets = userInfo.map(x => (x._1, x._2.toList.length))
    (userTweets)
  }


  /**
   *  Top 10 twitterers
   */
  def topTenTwitterers(): Array[(String, Int)] = {

    // Return the top 10 of persons which used to twitt the more
    // TODO write code here
    // Hint: the Spark API provides a sortBy method
    val sorted = tweetByUserNumber.sortBy(_._2, false).take(10)
    (sorted)
  }
  
    def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val results = topTenTwitterers
    val answer = results
    answer.foreach(println)
  }


}

