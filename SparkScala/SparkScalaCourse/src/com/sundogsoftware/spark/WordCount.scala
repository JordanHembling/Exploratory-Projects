package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("../book.txt")
    
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split("\\W+"))
    
    // to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count up the occurrences of each word (returns map object)
    // val wordCounts = lowercaseWords.countByValue()
    
    // *** OR *** count by value but return RDD    ----> so we can sort by count
    val wordCounts2 = lowercaseWords.map(x => (x,1)).reduceByKey( (x,y) => x + y)
    // flip order of key/value tuple and sort 
    val wordCountsSorted = wordCounts2.map(x => (x._2, x._1) ).sortByKey() 
    
    //collect result so that output returns ordered list of records amongst all partitions 
    val wordsFinal = wordCountsSorted.collect()
    
    // Print the results.
    for (result <- wordsFinal) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
  
}

