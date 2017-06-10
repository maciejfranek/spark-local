package com.datawizards.sparklocal.impl.scala.`lazy`.dataset

import com.datawizards.sparklocal.dataset.{DataSetAPI, KeyValueGroupedDataSetAPI}
import com.datawizards.sparklocal.impl.scala.dataset.DataSetAPIScalaBase
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.collection.{GenIterable, SeqView}
import scala.reflect.ClassTag

object DataSetAPIScalaLazyImpl {
  private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]): DataSetAPIScalaBase[U] =
    new DataSetAPIScalaLazyImpl(it.toSeq.seq.view)
}

class DataSetAPIScalaLazyImpl[T: ClassTag](private[sparklocal] val data: SeqView[T, Seq[T]]) extends DataSetAPIScalaBase[T] {
  override type InternalCollection = SeqView[T, Seq[T]]

  override private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]): DataSetAPIScalaBase[U] =
    DataSetAPIScalaLazyImpl.create(it)

  override protected def union(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.union(dsScala.data.toSeq))

  override protected def intersect(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.intersect(dsScala.data.toSeq))

  override protected def diff(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.diff(dsScala.data.toSeq))

  override def distinct(): DataSetAPI[T] =
    create(data.distinct)

  override def groupByKey[K: ClassTag](func: (T) => K)(implicit enc: Encoder[K]): KeyValueGroupedDataSetAPI[K, T] =
    new KeyValueGroupedDataSetAPIScalaLazyImpl(data.groupBy(func))

  override def rdd(): RDDAPI[T] = RDDAPI(data)
}
