import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object filterfile {  
  def main(args: Array[String]) {
   val logFile = "D:/Drive/BigData/CONA/Data/sample.txt"
  // val conf = new SparkConf().setAppName("Spark Wordcount")
   val conf = new SparkConf().setAppName("Spark Wordcount").setMaster("local[2]").set("spark.executor.memory","1g");
   val sc = new SparkContext(conf)
   val logData = sc.textFile(logFile, 2).cache()
   val numAs = logData.filter(line => line.contains("ERROR")).count()
   val numBs = logData.filter(line => line.contains("WARNING")).count()
   val numCs = logData.filter(line => line.contains("INFO")).count()
     println("Lines with ERROR: %s, Lines with WARNING: %s".format(numAs, numBs))     
  }
}