package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.explode

object burtonJSON {
  
    def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext without much actual configuration
    // We want EMR's config defaults to be used.
    val spark = SparkSession
      .builder
      .appName("burtonJSON")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    val jsonLines = spark.read.format("json").option("mode", "DROPMALFORMED").load("../7513_profile.20190125.json")
    val jsonLines2 = spark.read.json("../7513_profile.20190125.json")
   

    jsonLines.select("*").show(5)
     //jsonLines2.select("*").show(5)
    
    //jsonLines.printSchema()
    //jsonLines2.printSchema()
    
    val count = jsonLines.count()
    val count2 = jsonLines2.count()
    
    //println(count)
    //println(count2)
    
    val jsonExplode = jsonLines2.select($"id", explode($"browser"))
    jsonExplode.show(5)
}
}