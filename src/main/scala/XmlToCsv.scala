import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.XML

object XmlToCsv {
  val inputFile = "D:\\dataset\\customer.xml"

  def parseXML(content: String): Seq[(String, String, String, String, String)] = {
    val xml = XML.loadString(content)
    val employees = (xml \ "employee").map { emp =>
      val empId = (emp \ "empId").text
      val empName = (emp \ "empName").text
      val empSalary = (emp \ "empSalary").text
      val empCompany = (emp \ "empCompany").text
      val empEmail = (emp \ "empEmail").text
      (empId, empName, empSalary, empCompany, empEmail)
    }
    employees
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val inputXmlRDD = sc.wholeTextFiles(inputFile)
    inputXmlRDD.collect.foreach { case (path, content) => println(s"path= $path and content=$content") }

    val parsedData = inputXmlRDD.flatMap { case (path, content) =>
      val employees = parseXML(content)
      employees.map { case (empId, empName, empSalary, empCompany, empEmail) =>
        s"$empId,$empName,$empSalary,$empCompany,$empEmail"
      }
    }
    parsedData.collect.foreach(println)

    // Save the CSV data to a single CSV file
    parsedData.coalesce ( 1 ).saveAsTextFile ( "file:///D:/dataset/customer_output.csv" )

  }
}
