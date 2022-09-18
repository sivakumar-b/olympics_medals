package olympics_medals.project
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
class findmedalsclass {
   @transient lazy val logger=Logger.getLogger(getClass.getName)
   def findmedalswon(textFile:RDD[String]):Unit={
     logger.info("Inside find medals class")

  //schema check - if columns are more than 10 then should throw false
    val counts=textFile.filter{x=>{if(x.toString().split(",").length>=9)true else false}}.map(line=>{line.toString().split(",")})
    counts.foreach(println)
    //gold medals won in swimming
    val fil=counts.filter{x=>{if (x(4).equalsIgnoreCase("swimming")&&(x(5).matches("\\d+")))true else false}}
    //fil.foreach(println)
    println(fil.count())
    //total medals won by each country
    val pairs: RDD [(String,Int)] = fil.map(x=>(x(2),x(8).toInt)) 
    pairs.foreach(println)
    val cnt=pairs.reduceByKey(_+_)
    println()
    cnt.foreach(println)
    println()
}
}