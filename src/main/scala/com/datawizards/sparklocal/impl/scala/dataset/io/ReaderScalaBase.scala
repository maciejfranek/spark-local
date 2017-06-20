package com.datawizards.sparklocal.impl.scala.dataset.io

import java.io.File
import java.sql.DriverManager

import com.datawizards.csv2class
import com.datawizards.csv2class._
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.{Reader, ReaderExecutor}
import com.datawizards.sparklocal.datastore
import com.datawizards.sparklocal.datastore.FileDataStore
import com.datawizards.jdbc2class._
import com.sksamuel.avro4s._
import org.apache.avro.file.{DataFileReader, SeekableFileInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.spark.sql.Encoder
import org.json4s._
import org.json4s.native.JsonMethods._
import shapeless.Generic.Aux
import shapeless.HList

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait ReaderScalaBase extends Reader {
  override def read[T]: ReaderExecutor[T] = new ReaderExecutor[T] {
    override def apply[L <: HList](dataStore: datastore.CSVDataStore)
                                  (implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {
      genericFileRead(dataStore) { file =>
        parseCSV[T](
          path = file.getPath,
          delimiter = dataStore.delimiter,
          quote = dataStore.quote,
          escape = dataStore.escape,
          header = dataStore.header,
          columns = dataStore.columns
        )._1
      }
    }

    override def apply(dataStore: datastore.JsonDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T]): DataSetAPI[T] = {
      implicit val formats = DefaultFormats

      genericFileRead(dataStore) { file =>
        scala.io.Source
          .fromFile(file)
          .getLines()
          .map(line => parse(line).extract[T])
          .toIterable
      }
    }

    override def apply(dataStore: datastore.ParquetDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], enc: Encoder[T]): DataSetAPI[T] = {
      val format = RecordFormat[T]

      genericFileRead(dataStore) { file =>
        val reader = AvroParquetReader.builder[GenericRecord](new Path(file.getPath)).build()
        val iterator = Iterator.continually(reader.read).takeWhile(_ != null).map(format.from)
        iterator.toList
      }
    }

    override def apply(dataStore: datastore.AvroDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], fromRecord: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] = {
      genericFileRead(dataStore) { file =>
        val datumReader = new GenericDatumReader[GenericRecord]()
        val dataFileReader = new DataFileReader[GenericRecord](new SeekableFileInput(file), datumReader)
        val buffer = new ListBuffer[T]
        while(dataFileReader.hasNext)
          buffer += fromRecord(dataFileReader.next)
        buffer.toList
      }
    }

    override def apply(dataStore: datastore.HiveDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] =
      apply(datastore.AvroDataStore(dataStore.localFilePath))

    override def apply[L <: HList](dataStore: datastore.JdbcDataStore)
                                  (implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {
      Class.forName(dataStore.driverClassName)
      val connection = DriverManager.getConnection(dataStore.url, dataStore.connectionProperties)
      val result = selectTable[T](connection, dataStore.fullTableName)
      connection.close()
      createDataSet(result._1)
    }

    private def genericFileRead(dataStore: FileDataStore)
                               (readFile: File=>Iterable[T])
                               (implicit ct: ClassTag[T]): DataSetAPI[T] = {
      def readAllFilesFromDirectory(directory: File): Iterable[T] = {
        directory
          .listFiles()
          .filter(f => f.getName.endsWith(dataStore.extension))
          .map(f => readFile(f))
          .reduce(_ ++ _)
      }

      val file = new File(dataStore.path)
      val data = if(file.isDirectory) readAllFilesFromDirectory(file) else readFile(file)

      createDataSet(data)
    }
  }

  protected def createDataSet[T: ClassTag](iterable: Iterable[T]): DataSetAPI[T]

}
