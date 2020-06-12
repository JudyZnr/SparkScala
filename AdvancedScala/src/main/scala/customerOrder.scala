import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object customerOrder {

  def main(args: Array[String])={
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "customerOrder")

    // Read in each rating line
    val input = sc.textFile("/Users/nairongzhang/Downloads/SparkScala3/customer-orders.csv")
//    val mappedInput = input.map(parseLines)
//
//    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )
//
//    val flipped = totalByCustomer.map( x => (x._2, x._1) )
//
//    val totalByCustomerSorted = flipped.sortByKey()
//
//    val results = totalByCustomerSorted.collect()

    // Print the results.
    val customerOrder = input.map(parseLines)
    val results = customerOrder.reduceByKey((x,y) => x+y).map(x=> (x._2,x._1)).sortByKey(false)

    val sortedRes = results.collect()
    sortedRes.foreach(println)
  }

  def parseLines (lines:String) ={
    val fields = lines.split(",")
    (fields(0).toInt , fields(2).toFloat)
  }

}
