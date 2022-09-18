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

object demo {
  @transient lazy val logger=Logger.getLogger(getClass.getName)
  def main (args: Array[String])={
    logger.info("Inside main client class")
    val conf= new SparkConf().setAppName("olympics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("execution started")
    sc.setLogLevel("error")
    if(args.length<2)
    {
      println("usage:scala count")
      System.exit(1)
    }
    val spark=SparkSession
    .builder()
    .appName("endtoendproject")
    .config("Spark.some.config.option","somevalue")
    .getOrCreate()
    import spark.implicits._
    val r1=sc.textFile(path="C:/Datasets/olympicsmedals.csv")
    val sp=new findmedalsclass()
    sp.findmedalswon(r1)
    
  }
}