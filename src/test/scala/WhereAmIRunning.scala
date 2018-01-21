package ppt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WhereAmIRunning {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WhereAmI")
    val sc = new SparkContext(conf)
    if (sc.isLocal) println("################## I, Local ###################")
    else println("################## I, Cluster ###################")
  }
}