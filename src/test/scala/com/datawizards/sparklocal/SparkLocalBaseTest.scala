package com.datawizards.sparklocal

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.FunSuite

trait SparkLocalBaseTest extends FunSuite {
  lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  lazy val sc: SparkContext = spark.sparkContext
  lazy val sqlContext: SQLContext = spark.sqlContext

  def assertDatasetOperation[T:Manifest, That](data: Seq[T])(op: DataSetAPI[T] => DataSetAPI[That]): Unit = {
    val ds = spark.createDataset(data)(ExpressionEncoder[T]())

    assert(op(DataSetAPI(data)) == op(DataSetAPI(ds)))
  }

}