package com.sundogsoftware.spark.dataframe

import org.apache.spark.{SparkContext, SparkConf}
import com.sundogsoftware.spark.utils._
import com.sundogsoftware.spark.utils.TweetUtils.Tweet
import org.apache.log4j._
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame

/**
 *  The Spark SQL and DataFrame documentation is available on:
 *  https://spark.apache.org/docs/1.4.0/sql-programming-guide.html
 *
 *  A DataFrame is a distributed collection of data organized into named columns.
 *  The entry point before to use the DataFrame is the SQLContext class (from Spark SQL).
 *  With a SQLContext, you can create DataFrames from:
 *  - an existing RDD
 *  - a Hive table
 *  - data sources...
 *
 *  In the exercise we will create a dataframe with the content of a JSON file.
 *
 *  We want to:
 *  - print the dataframe
 *  - print the schema of the dataframe
 *  - find people who are located in Paris
 *  - find the user who tweets the more
 * 
 *  And just to recap we use a dataset with 8198 tweets,where a tweet looks like that:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 * 
 *  Use the DataFrameOnTweetsSpec to implement the code.
 */
object DataFrameOnTweets {


  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Here the method to create the contexts (Spark and SQL) and
   *  then create the dataframe.
   *
   *  Run the test to see how looks the dataframe!
   */
  def loadData(): DataFrame = {
    // create spark configuration and spark context
//    val conf = new SparkConf()
//        .setAppName("Dataframe")
//        .setMaster("local[*]")
//
//    val sc = new SparkContext(conf)

    // Create a sql context: the SQLContext wraps the SparkContext, and is specific to Spark SQL.
    // It is the entry point in Spark SQL.
    // TODO write code here
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Load the data regarding the file is a json file
    // Hint: use the sqlContext and apply the read method before loading the json file
    // TODO write code here
    val tweets = spark.read.json(pathToFile)
    (tweets)
  }


  /**
   *  See how looks the dataframe
   */
  def showDataFrame() = {
    val dataframe = loadData

    // Displays the content of the DataFrame to stdout
    dataframe.select("*").show()
  }

  /**
   * Print the schema
   */
  def printSchema() = {
    val dataframe = loadData()

    // Print the schema
    dataframe.printSchema()
  }

  /**
   * Find people who are located in Paris
   */
  def filterByLocation(): DataFrame = {
    val dataframe = loadData()

    // Select all the persons which are located in Paris
    // TODO write code here
    val paris = dataframe.where(dataframe("country") === "Paris")
    (paris)
  }


  /**
   *  Find the user who tweets the more
   */
  def mostPopularTwitterer(): Unit = {
    val dataframe = loadData()

    // First group the tweets by user
    // Then sort by descending order and take the first one
    // TODO write code here
    val grouped = dataframe.groupBy(dataframe("user")).count()
    
    grouped.select("*").sort(grouped("count").desc).head(10).foreach(println)

  }

   def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    showDataFrame
    printSchema
    mostPopularTwitterer
  }
  
  
}
