package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.ParquetDataStore
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteReadParquetTest extends SparkLocalBaseTest {

  val data = Seq(
    Person("p1", 10),
    Person("p2", 20),
    Person("p3", 30),
    Person("p4", 40)
  )

  test("Writing and reading file produces the same result - Scala") {
    val file = "target/people_scala.parquet"
    val expected = DataSetAPI(data)
    val dataStore = ParquetDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing and reading file produces the same result - Spark") {
    import spark.implicits._
    val file = "target/people_spark.parquet"
    val expected = DataSetAPI(data.toDS())
    val dataStore = ParquetDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Trying to write to existing file should throw exception - Scala") {
    val file = "target/people_scala_error.parquet"
    val expected = DataSetAPI(data)
    val dataStore = ParquetDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    intercept[Exception] {
      expected.write(dataStore, SaveMode.ErrorIfExists)
    }
  }

  test("Trying to write to existing file should throw exception - Spark") {
    import spark.implicits._
    val file = "target/people_spark_error.parquet"
    val expected = DataSetAPI(data.toDS())
    val dataStore = ParquetDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    intercept[Exception] {
      expected.write(dataStore, SaveMode.ErrorIfExists)
    }
  }

  test("Writing twice to same file produces single result - Scala") {
    val file = "target/people_scala_twice.parquet"
    val expected = DataSetAPI(data)
    val dataStore = ParquetDataStore(file)
    expected.union(expected).write(dataStore, SaveMode.Overwrite)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing twice to same file produces single result - Spark") {
    import spark.implicits._
    val file = "target/people_spark_twice.parquet"
    val expected = DataSetAPI(data.toDS())
    val dataStore = ParquetDataStore(file)
    expected.union(expected).write(dataStore, SaveMode.Overwrite)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Writing to existing file with ignore - Scala") {
    val file = "target/people_scala_ignore.parquet"
    val expected = DataSetAPI(data)
    val dataStore = ParquetDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    val ignored = expected.union(expected)
    ignored.write(dataStore, SaveMode.Ignore)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing to existing file with ignore - Spark") {
    val file = "target/people_spark_ignore.parquet"
    val expected = DataSetAPI(data.toDS())
    val dataStore = ParquetDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    val ignored = expected.union(expected)
    ignored.write(dataStore, SaveMode.Ignore)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Writing twice to same file with append - Scala") {
    val file = "target/people_scala_twice.parquet"
    val single = DataSetAPI(data)
    val expected = single.union(single)
    val dataStore = ParquetDataStore(file)
    single.write(dataStore, SaveMode.Overwrite)
    single.write.parquet(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person].parquet(dataStore)
    }
  }

  test("Writing twice to same file with append - Spark") {
    import spark.implicits._
    val file = "target/people_spark_twice.parquet"
    val single = DataSetAPI(data.toDS())
    val expected = single.union(single)
    val dataStore = ParquetDataStore(file)
    single.write(dataStore, SaveMode.Overwrite)
    single.write.parquet(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderSparkImpl.read[Person].parquet(dataStore)
    }
  }

}