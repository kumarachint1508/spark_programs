import org.apache.spark.{SparkContext,SparkConf}

object CustomerAnalysis_Assigmnet {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local").setAppName("JA mila")
    val sc=new SparkContext(conf)

    val idCustomerKVFile=("D:\\output\\dataset\\customerDetails\\customerDetails.txt")
    val idCustomerBalanceKVFile=("D:\\output\\dataset\\customerDetails\\customerBalance - Copy.txt")

    val idCustomerRDD=sc.textFile(idCustomerKVFile)
    val idCustomerBalanceRDD=sc.textFile(idCustomerBalanceKVFile)

    /*idCustomerRDD.foreach(println)
    idCustomerBalanceRDD.foreach(println)*/

    val idCusKV=idCustomerRDD.map(row => {
      val data=row.split(",")
      (data(0),data(1)+" "+data(2))
    })
    //idCusKV.collect.foreach(println)

    val idCusBalanceKV=idCustomerBalanceRDD.map(row => {
      val data=row.split(",")
      val key = data(0)
      val value = if (data.length > 1) Some ( data ( 1 ) ) else None
      (key, value)
    })
  //idCusBalanceKV.collect().foreach(println)

    val joined=idCusKV.fullOuterJoin(idCusBalanceKV)
    joined.collect.foreach{
      case (key, (Some ( name ), Some ( Some ( balance ) ))) => println ( s"$key,($name,$balance)" )
      case (key, (Some ( name ), Some ( None ))) => println ( s"$key,($name,)" )
    }

  }

}
