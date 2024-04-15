import org.apache.spark.{SparkContext,SparkConf}

object _3FIleWiseLineWiseAvg {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("My App")
    val sc=new SparkContext(conf)

    /*val numFileRDD=sc.wholeTextFiles("D:\\dataset\\filewiseaddition\\num*.txt")
    val result = numFileRDD.flatMap { case (_, content) =>
      val lines = content.split ( "\n" )
      val avgPerFile = lines.map { line =>
        val numStringArr = line.split ( "," )
        val numDoubleArr = numStringArr.map ( num => num.trim.toDouble )
        val lineSum = numDoubleArr.sum
        val lineAvg = lineSum / numDoubleArr.length
        lineAvg
      }
      avgPerFile
    }
    val totalSum = result.reduce ( _ + _ )
    println ( "Average per line per file:" )
    result.collect ().foreach ( println )
    println ( "Total sum of all averages: " + totalSum )
*/

    val numFileRDD = sc.wholeTextFiles ( "D:\\dataset\\filewiseaddition\\num*.txt" )
    val result = numFileRDD.flatMap { case (filePath, content) =>
      val lines = content.split ( "\n" )
      val fileLinesAvg = lines.zipWithIndex.map { case (line, lineIndex) =>
        val numStringArr = line.split ( "," )
        val numDoubleArr = numStringArr.map ( num => num.trim.toDouble )
        val lineSum = numDoubleArr.sum
        val lineAvg = lineSum / numDoubleArr.length
        (filePath, lineIndex + 1, lineAvg)
      }
      fileLinesAvg
    }

    println ( "Expected Output:" )
    val groupedByFile = result.groupBy ( _._1 )

    groupedByFile.foreach { case (file, data) =>
      println ( file )
      data.foreach { case (_, lineIndex, lineAvg) =>
        println ( s"Line$lineIndex Avg: $lineAvg" )
      }
      println ()
    }

    val totalSum = result.map { case (_, _, lineAvg) => lineAvg }.reduce ( _ + _ )
    println ( "Total sum of all averages: " + totalSum )

  }

}
