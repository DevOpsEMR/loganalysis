import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object filterfile {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spark Wordcount").setMaster("local[2]").set("spark.executor.memory","1g");
    val sc = new SparkContext(conf)
    //

    //
    val pathToFiles = "s3://devopsemr/server_system_log.txt"
    val outputPath = "s3://devopsemr/emrjoboutput.txt"
    val files = sc.textFile(pathToFiles)
   
    //Save the file
    val fileWithError = files.filter(line => line.contains("WARN"))
    fileWithError.saveAsTextFile(outputPath) 
  }

}
