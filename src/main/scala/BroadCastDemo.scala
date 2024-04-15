import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Employee(empNo:Int, empName:String, empJob:String, empSalary:Float, empMgr:String, deptNo:Int)

case class Department(deptNo:Int , deptName:String, deptLoc:String)

object BroadCastDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("My App")
    val sc=new SparkContext(conf)

    val empFileRDD=sc.textFile("D:\\output\\dataset\\empDept\\employee.txt")
    val deptFileRDD=sc.textFile("D:\\output\\dataset\\empDept\\dept.txt")

    /*empFileRDD.foreach(println)
    deptFileRDD.foreach(println)*/

    val empRDD=empFileRDD.map(emp => {
      val data=emp.split(",")
      Employee(data(0).toInt,data(1),data(2),data(3).toFloat,data(4),data(5).toInt)
    })
    val deptRDD=deptFileRDD.map(dept => {
      val data =dept.split(",")
      (data(0).toInt,Department(data(0).toInt, data(1),data(2)))
    })
    //println(deptRDD.collectAsMap())
    val broadCastDept=sc.broadcast(deptRDD.collectAsMap())

    val empDeptRDD=empRDD.mapPartitions({ it => {
      val deptMap=broadCastDept.value
      println(s"Inside map partitions :: ${deptMap}")
      it.map( emp => {
        val dept=deptMap.getOrElse(emp.deptNo, null)
        if(dept !=null){
          (emp.empNo, emp.empName,emp.empJob,emp.empMgr,emp.empSalary,dept.deptName,dept.deptLoc,dept.deptNo)
        }
      } )
    }
    },preservesPartitioning = true)
    empDeptRDD.foreach(println)
    Thread.sleep((600000))
  }
}