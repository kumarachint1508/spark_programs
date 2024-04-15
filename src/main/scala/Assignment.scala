import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext,SparkConf}



object Assignment {
  val inputFile="D:\\output\\txn.txt"
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("My App")
    val sc=new SparkContext(conf)
    val inputRDD=sc.textFile(inputFile)
    println(s"Total Lines: ${inputRDD.count()}")

    val contentArr:Array[String]=inputRDD.collect()
    println("content :" )
    contentArr.foreach(println)
    /*inputRDD.foreach(println)*/

  }
}
