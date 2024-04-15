
import org.apache.spark.{SparkContext,SparkConf}

object _2FileLineWiseSum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf ().setMaster ( "local" ).setAppName ( "My App" )
    val sc = new SparkContext ( conf )

    // sum of all numbers per line per file
   /* val numFileRDD=sc.wholeTextFiles("D:\\dataset\\filewiseaddition\\num*.txt")
    val result=numFileRDD.flatMap { case (_, content) =>
      val lines = content.split ( "\n" )
      val linesums=lines.map{ line =>
        val numStringArr=line.split(",")
        val numDoubleArr= numStringArr.map(num => num.trim.toDouble)
        numDoubleArr.sum
      }
      linesums
    }
    val totalSum=result.reduce(_+_)
    println ( "Line-wise sums:" )
    result.collect ().foreach ( println )
    println ( "Total sum of all line-wise sums: " + totalSum )
    }*/

    val numFileRDD = sc.wholeTextFiles ( "D:\\dataset\\filewiseaddition\\num*.txt" )
    val result = numFileRDD.flatMap { case (_, content) =>
      val lines = content.split ( "\n" )
      lines.zipWithIndex.map { case (line, index) =>
        val numStringArr = line.split ( "," )
        val numDoubleArr = numStringArr.map ( num => num.trim.toDouble )
        (index, numDoubleArr.sum)
      }
    }
    val lineSums = result.reduceByKey ( _ + _ ).sortBy ( _._1 )
    lineSums.foreach { case (lineIndex, sum) =>
      println ( s"Line ${lineIndex + 1}: $sum" )
    }

  }
}