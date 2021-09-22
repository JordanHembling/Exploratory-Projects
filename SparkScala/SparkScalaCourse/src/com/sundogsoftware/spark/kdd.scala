package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Find the movies with the most ratings. */
object kdd {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "kdd")   
    
    // Read in each rating line
    val lines = sc.textFile("../kddcup.data")
    
    // Map to (movieID, 1) tuples
    val fields = lines.map(x => x.toString().split(","))

    // Count up all the 1's for each movie
    for(line <- fields) {
      val one = line(0)
      val two = line(1)
      val three = line(2)
      println(s"$one, $two, $three")
    }
  
}
}

