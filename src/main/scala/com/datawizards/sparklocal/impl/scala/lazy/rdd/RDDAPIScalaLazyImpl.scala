package com.datawizards.sparklocal.impl.scala.`lazy`.rdd

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.scala.rdd.RDDAPIScalaBase
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.collection.{GenIterable, SeqView}
import scala.reflect.ClassTag

class RDDAPIScalaLazyImpl[T: ClassTag](private[sparklocal] val data: SeqView[T, Seq[T]]) extends RDDAPIScalaBase[T] {
  override type InternalCollection = SeqView[T, Seq[T]]

  override private[sparklocal] def create[U: ClassTag](data: GenIterable[U]): RDDAPIScalaBase[U] =
    new RDDAPIScalaLazyImpl(data.toSeq.seq.view)

  override protected def union(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T] =
    create(data.union(rddScala.data.toSeq))

  override protected def intersect(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T] =
    create(data.intersect(rddScala.data.toSeq))

  override protected def diff(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T] =
    create(data.diff(rddScala.data.toSeq))

  override def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] =
    data.sorted.take(num).toArray

  override def cache(): RDDAPI[T] =
    create(data.force.view)

  override def sortBy[K](f: (T) => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDDAPI[T] =
    create(data.sortBy(f)(if(ascending) ord else ord.reverse))

  override def distinct(): RDDAPI[T] =
    create(data.distinct)

  override def top(num: Int)(implicit ord: Ordering[T]): Array[T] =
    data.sorted(ord.reverse).take(num).toArray

  override def toDataSet(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(data)
}
