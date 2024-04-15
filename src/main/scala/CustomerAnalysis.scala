import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object CustomerAnalysis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf ().setMaster ( "local" ).setAppName ( "My App1" )
    val sc = new SparkContext ( conf )

    val customerDataFile = "D:\\output\\dataset\\customerDetails\\customerDetails.txt"
    val customerBalanceFile = "D:\\output\\dataset\\customerDetails\\customerBalance.txt"

    val detailsRDD = sc.textFile ( customerDataFile )
    val balanceRDD = sc.textFile ( customerBalanceFile )

    /*detailsRDD.foreach(println)
    balanceRDD.foreach(println)*/
    val idDetailsKeyValue = detailsRDD.map ( row => {
      val data = row.split ( "," )
      (data ( 0 ), data ( 1 ) + " " + data ( 2 ))
    } )
    //idDetailsKeyValue.foreach(println)

    val idBalanceKeyValue = balanceRDD.map ( row => {
      val data = row.split ( "," )
      (data ( 0 ), data ( 1 ))
    } )
    //idBalanceKeyValue.foreach ( println )
    val joined = idDetailsKeyValue.join ( idBalanceKeyValue )
    //joined.foreach(println)

    val joined1 = idDetailsKeyValue.join ( idBalanceKeyValue ).sortByKey ( false )
    //joined1.collect.foreach(println)

    val joined2 = idDetailsKeyValue.join ( idBalanceKeyValue ).sortByKey ()
    //joined2.foreach(println)

    val finalOutput = joined2.map { case (accno, (name, amount)) => (accno, name, amount) }
    //finalOutput.collect.foreach(println)

    val joined3 = idDetailsKeyValue.fullOuterJoin ( idBalanceKeyValue )
    //joined3.collect.foreach(println)

    val pairedByBalance = finalOutput.map {
      case (accno, name, amount) =>
        (amount.toFloat, (accno, name))
    }
    //pairedByBalance.collect.sorted.foreach ( println )


    val sortedByBalance = pairedByBalance.sortByKey ( false )
    //sortedByBalance.foreach ( println )

    val highestSal = sortedByBalance.first
    //println(highestSal)

    val highestBalance = sortedByBalance.first ()
    /*println ( highestBalance._2 + " " + highestBalance._1 )
    println ( highestBalance._2._1 + " " + highestBalance._2._2 + " " + highestBalance._1 )
*/
    val top3BalanceCustomer = sortedByBalance.top(3)
    top3BalanceCustomer.foreach(println)
    val formatTop3=top3BalanceCustomer.map {
      case (balance, (accNo, name)) => (accNo, name, balance)
    }
    //formatTop3.foreach(println)

    idBalanceKeyValue.collect.foreach(println)

    val onlyBalanceRDD=idBalanceKeyValue.map{ case(_,balance) => balance.toFloat}
    //println("Total Balance :"+onlyBalanceRDD.sum)
    //println("max Balance :"+onlyBalanceRDD.max)

    println(" toatl balan " + idBalanceKeyValue.map(_._2.toInt).sum)

    //using accumulator
    val balanceTotal=sc.longAccumulator("Account Balance Total")
    idBalanceKeyValue.foreach(data => balanceTotal.add(data._2.toInt))
    println("Using accumulator "+ balanceTotal.value)
    }
}
