package sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(List("tv 17999 1000",
      "xiaomi  200 10",
      "xiaomiMonitor 200 1000",
      "mitu 9 2000"), 1)

    // First, sort the items in descending order based on their prices.
    // If the prices are the same, sort them in descending order based on their inventory.
    // Solution 1：tuple
    data.map(x => {
      val splits = x.split(" ")
      val name = splits(0)
      val price = splits(1).toInt
      val store = splits(2).toInt
      (name, price, store)
    }).sortBy(x => (-x._2, -x._3)).foreach(println)

    // Solution 2: class
    val products: RDD[ProductClass] = data.map(x => {
      val splits = x.split(" ")
      val name = splits(0)
      val price = splits(1).toInt
      val store = splits(2).toInt
      new ProductClass(name, price, store)
      //ProductCaseClass(name, price, store)
    })

    implicit def productClass2Ordered(productClass: ProductClass):Ordered[ProductClass] = new Ordered[ProductClass]{
      override def compare(that: ProductClass): Int = {
        that.price - productClass.price
      }
    }

    sc.stop()
  }
}

case class ProductCaseClass (val name:String, val price:Int, val store:Int)
  extends Ordered[ProductCaseClass] {
  override def compare(that: ProductCaseClass): Int = {
    that.price - this.price
  }
}
class ProductClass(val name:String, val price:Int, val store:Int)
  extends Serializable{
  override def toString = s"ProductClass($name, $price, $store)"
}
