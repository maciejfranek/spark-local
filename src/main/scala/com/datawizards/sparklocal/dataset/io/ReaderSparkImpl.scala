package com.datawizards.sparklocal.dataset.io
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import com.databricks.spark.avro._
import com.datawizards.csv2class
import shapeless.Generic.Aux
import shapeless.HList

object ReaderSparkImpl extends Reader {
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  override def read[T]: ReaderExecutor[T] = new ReaderExecutor[T] {
    override def apply[L <: HList](dataStore: datastore.CSVDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L]): DataSetAPI[T] = {
      var df = spark
        .read
        .option("header", dataStore.header.toString)
        .option("inferSchema", value = true)
        .option("delimiter", dataStore.delimiter.toString)
        .option("quote", dataStore.quote.toString)
        .option("escape", dataStore.escape.toString)
        //.option("charset", dataStore.charset)
        .csv(dataStore.path)

      if(!dataStore.header) {
        df = df.toDF(dataStore.columns: _*)
      }

      DataSetAPI(df.as[T](ExpressionEncoder[T]()))
    }

    override def apply[L <: HList](dataStore: datastore.JsonDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T]): DataSetAPI[T] =
      DataSetAPI(
        spark
          .read
          .json(dataStore.path)
          .as[T](ExpressionEncoder[T]())
      )

    override def apply[L <: HList](dataStore: datastore.ParquetDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L]): DataSetAPI[T] =
      DataSetAPI(
        spark
          .read
          //TODO - options
          .parquet(dataStore.path)
          .as[T](ExpressionEncoder[T]())
      )

    override def apply[L <: HList](dataStore: datastore.AvroDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L]): DataSetAPI[T] =
      DataSetAPI(
        spark
          .read
          //TODO - options
          .avro(dataStore.path)
          .as[T](ExpressionEncoder[T]())
      )
  }

}