package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PurchaseByCustomer {
  
  def parseLine(line:String) = {
    val fields = line.split(",")
    val customerID = fields(0).toInt
    val total = fields(2).toFloat
    (customerID, total)
  }
  
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")
    
    // Read each line of input data
    val input = sc.textFile("../customer-orders.csv")
    
    // parse data and select fields 
    val parsedData = input.map(parseLine)
    
    //reduce 
    val customerSpend = parsedData.reduceByKey( (x,y) => x+y)
    
    // sort by value
    val spendCustomer = customerSpend.map(x => (x._2, x._1) ).sortByKey()
    
    //collect
    val totals = spendCustomer.collect()
    
    // print 
    for(result <- totals) {
      val customerID = result._2
      val total = result._1
      
      println(f"$customerID: $total%.2f")
    }
  }
}