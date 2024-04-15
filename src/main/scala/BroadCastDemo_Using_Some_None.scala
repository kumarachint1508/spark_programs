import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Employee1(empNo: Int, empName: String, empJob: String, empSalary: AnyVal, empMgr: String, deptNo: Int)

case class Department1(deptNo: Int, deptName: String, deptLoc: String)

object BroadCastDemo_Using_Some_None {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val empFileRDD = sc.textFile("D:\\output\\dataset\\empDept\\employee.txt")
    val deptFileRDD = sc.textFile("D:\\output\\dataset\\empDept\\dept.txt")

    val empRDD = empFileRDD.flatMap(emp => {
      val data = emp.split(",")      //using Some
      val empNo = Some(data(0).toInt)
      val empSalary = Some(data(3).toFloat)
      val deptNo = Some(data(5).toInt)
      //Using Some And None
   /*   val data=emp.split((","))
      val empNo=if(data(0).isEmpty) None else Some(data(0).toInt)
      val empSalary=if(data(3).isEmpty) None else Some(data(3).toFloat)
      val deptNo=if(data(5).isEmpty)None else Some(data(3).toInt)*/
      //Some(Employee1(empNo.getOrElse(0), data(1), data(2), empSalary.getOrElse(0.0), data(4), deptNo.getOrElse(0)))
      Some(Employee1(empNo.getOrElse(0), data(1), data(2), empSalary.getOrElse(0.0), data(4), deptNo.getOrElse(0)))
    })

    val deptRDD = deptFileRDD.map(dept => {
      val data = dept.split(",")
      (data(0).toInt, Department1(data(0).toInt, data(1), data(2)))
    }).collectAsMap()

    val broadCastDept = sc.broadcast(deptRDD)

    val empDeptRDD = empRDD.mapPartitions(it => {
      val deptMap = broadCastDept.value
      it.flatMap(emp => {
        val deptNo = emp.deptNo
        val dept = deptMap.get(deptNo)
        dept.map(d => (emp.empNo, emp.empName, emp.empJob, emp.empMgr, emp.empSalary, d.deptName, d.deptLoc, d.deptNo))
      })
    }, preservesPartitioning = true)

    empDeptRDD.foreach(println)
    Thread.sleep(1000)
  }
}
