import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object filterfile {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Wordcount").setMaster("local[2]").set("spark.executor.memory","1g");
    val sc = new SparkContext(conf)
    val pathToFiles = "https://s3.amazonaws.com/devopsemr/commafile.txt"
    val outputPath = "https://s3.amazonaws.com/devopsemr/output.txt"
    val files = sc.textFile(pathToFiles)
   
    val rowsWithoutSpaces = files.map(_.replaceAll(" ", ","))
    
    rowsWithoutSpaces.saveAsTextFile(outputPath)
  }

}