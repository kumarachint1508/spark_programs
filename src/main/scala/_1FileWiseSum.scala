import org.apache.spark.{SparkConf, SparkContext}

object _1FileWiseSum {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("My App")
    val sc=new SparkContext(conf)
    val numFileRDD=sc.wholeTextFiles("D:\\dataset\\filewiseaddition\\num*.txt")
    val result=numFileRDD.mapValues{ line =>
      println(s"line=$line")
      val numStringArr=line.split(",|\\n")
      //numStringArr.foreach(println)
      val numDoubleArr=numStringArr.map(num => num.trim.toDouble)
      numDoubleArr.sum/numDoubleArr.length
    }
    result.collect.foreach(println)
    //result.saveAsTextFile("D:\\dataset\\filewiseaddition\\sumFileWise")
  }

}
