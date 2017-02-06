import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object filterfile {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spark Wordcount").setMaster("local[2]").set("spark.executor.memory","1g");
    val sc = new SparkContext(conf)
    //
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId", "AKIAJBGUMHBIOXCXMCWA")
    hadoopConf.set("fs.s3.awsSecretAccessKey", "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")
    //
    val pathToFiles = "s3://devopsemr/commafile.txt"
    val outputPath = "s3://devopsemr/output.txt"
    val files = sc.textFile(pathToFiles)
    //replace  space with ,
    //val rowsWithoutSpaces = files.map(_.replaceAll(" ", ","))
    //Save the file
    val fileWithError = files.filter(line => line.contains("WARN"))
    fileWithError.saveAsTextFile(outputPath)
  }

}