package com.datawizards.sparklocal.example

import com.datawizards.sparklocal.DataSetAPI
import org.apache.spark.sql.SparkSession

object Example1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val data = Seq(1,2,3)
    val ds = data.toDS()

    assertEquals(
      calculateSum(DataSetAPI(data)),
      calculateSum(DataSetAPI(ds))
    )

    assertEquals(
      calculateSumOfSquares(DataSetAPI(data)),
      calculateSumOfSquares(DataSetAPI(ds))
    )

  }

  def assertEquals[T](r1:T, r2:T): Unit = {
    println(r1)
    assert(r1 == r2)
  }

  def calculateSum(ds: DataSetAPI[Int]): Int = ds.reduce(_ + _)
  def calculateSumOfSquares(ds: DataSetAPI[Int]): Int = ds.map(x=>x*x).reduce(_ + _)

}
