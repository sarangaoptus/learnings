package com.srsoft.sparkexamples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]){
    val conf = new SparkConf()
    .setAppName("Word Count")
    .setMaster("local")
    
    val sc = new SparkContext(conf)
    
      if(args.length < 2) {
      println("Missing Arguments !!!")
    }
    
    // Loading text fileFile object 
    val textFile = sc.textFile(args(0))

  // Split the words
    
    val words = textFile.flatMap(line => line.split(" "))
    
    val counts = words.map(word => (word,1))
    
    val wordcount = counts.reduceByKey(_ + _)
    
    val wordcount_sorted = wordcount.sortByKey()
    
    wordcount_sorted.foreach(println)
    
   wordcount_sorted.saveAsTextFile(args(1))
    
    
  }
  
  
  
}