import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.io.FileUtils
import java.io.File


object WordCountUsingSpark {
  val inputFile="D:\\dataset\\word1.txt"
  val outputFile="D:\\dataset\\output.txt"

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("My App")
    val sc=new SparkContext(conf)
    val inputRDD: RDD[String]=sc.textFile(inputFile)
    println(s"total lines : ${inputRDD.count()}")

    val contentArr:Array[String]=inputRDD.collect()
    println("content :")
    contentArr.foreach(println)
    val words:RDD[String]=inputRDD.flatMap(_.split(" "))
    val count1PerWords:RDD[(String,Int)]=words.map(word => (word,1) )
    val counts:RDD[(String,Int)]=count1PerWords.reduceByKey(_ + _)
    FileUtils.deleteQuietly(new File(outputFile))
    counts.saveAsTextFile(outputFile)
    println("Program executed successfully")
  }

}
