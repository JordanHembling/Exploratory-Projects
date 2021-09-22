
package com.sundogsoftware.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.sundogsoftware.spark.utils._
import com.sundogsoftware.spark.utils.TweetUtils.Tweet
import org.apache.log4j._

/**
 *  The scala Spark API documentation: http://spark.apache.org/docs/latest/api/scala/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. Here an example of a tweet:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the tweets:
 *  - Find all the persons mentioned on tweets
 *  - Count how many times each person is mentioned
 *  - Find the 10 most mentioned persons by descending order
 *
 *  Use the Ex2TweetMiningSpec to implement the code.
 */
object Ex2TweetMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  def loadData(): RDD[Tweet] = {
    // create spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("Tweet mining")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile)
        .mapPartitions(TweetUtils.parseFromJson(_))

  }

  /**
   *  Find all the persons mentioned on tweets (case sensitive)
   */
  def mentionOnTweet(): RDD[String] = {
    val tweets = loadData

    // Hint: think about separating the word in the text field and then find the mentions
    // TODO write code here
    val text = tweets.map(x => x.text)
    val words = text.flatMap(x => x.split(" "))
    val reg = """(@\w+)""".r
    //words.filter(x => x.contains("@"))
    words.filter(x => reg.pattern.matcher(x).matches)
    
  }

  /**
   *  Count how many times each person is mentioned
   */
  def countMentions(): RDD[(String, Int)] = {
    val mentions = mentionOnTweet

    // Hint: think about what you did in the wordcount example
    // TODO write code here
    val pairs = mentions.map(x => (x, 1)).reduceByKey((x,y) => x + y)
    (pairs)
  }

  /**
   *  Find the 10 most mentioned persons by descending order
   */
  def top10mentions(): Array[(String, Int)] = {

    // Hint: take a look at the sorting and take methods
    // TODO write code here
    countMentions.sortBy(_._2, false).take(10)
  }
  
    def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val results = top10mentions
    val answer = results
    answer.foreach(println)
  }

}
