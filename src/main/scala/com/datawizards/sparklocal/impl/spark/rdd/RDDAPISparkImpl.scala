package com.datawizards.sparklocal.impl.spark.rdd

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.scala.eager.rdd.RDDAPIScalaEagerImpl
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.rdd.{PartitionCoalescer, RDD}
import org.apache.spark.sql.Encoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, Partitioner}

import scala.collection.GenIterable
import scala.reflect.ClassTag

class RDDAPISparkImpl[T: ClassTag](val data: RDD[T]) extends RDDAPI[T] {

  private def create[U: ClassTag](rdd: RDD[U]) = new RDDAPISparkImpl(rdd)

  override private[sparklocal] def toRDD = data

  override def collect(): Array[T] =
    data.collect()

  override def map[That: ClassTag](map: (T) => That): RDDAPI[That] =
    create(data.map(map))

  override def filter(p: (T) => Boolean): RDDAPI[T] =
    create(data.filter(p))

  override def flatMap[U: ClassTag](func: (T) => TraversableOnce[U]): RDDAPI[U] =
    create(data.flatMap(func))

  override def reduce(func: (T, T) => T): T =
    data.reduce(func)

  override def fold(zeroValue: T)(op: (T, T) => T): T =
    data.fold(zeroValue)(op)

  override def head(): T =
    data.first()

  override def head(n: Int): Array[T] =
    data.take(n)

  override def isEmpty: Boolean =
    data.isEmpty

  override def zip[U: ClassTag](other: RDDAPI[U]): RDDAPI[(T, U)] = other match {
    case rddScala:RDDAPIScalaEagerImpl[U] => RDDAPI(data zip parallelize(rddScala.data))
    case rddSpark:RDDAPISparkImpl[U] => create(data zip rddSpark.data)
  }

  override def foreach(f: (T) => Unit): Unit =
    data.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit =
    data.foreachPartition(f)

  override def checkpoint(): RDDAPI[T] = {
    data.checkpoint()
    this
  }

  override def cache(): RDDAPI[T] =
    create(data.cache())

  override def persist(newLevel: StorageLevel): RDDAPI[T] =
    create(data.persist(newLevel))

  override def persist(): RDDAPI[T] =
    create(data.persist())

  override def unpersist(blocking: Boolean): RDDAPI[T] =
    create(data.unpersist(blocking))

  override def union(other: RDDAPI[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaEagerImpl[T] => RDDAPI(data union parallelize(rddScala.data))
    case rddSpark:RDDAPISparkImpl[T] => create(data union rddSpark.data)
  }

  override def zipWithIndex(): RDDAPI[(T, Long)] =
    create(data.zipWithIndex())

  override def min()(implicit ord: Ordering[T]): T =
    data.min

  override def max()(implicit ord: Ordering[T]): T =
    data.max

  override def partitions: Array[Partition] =
    data.partitions

  override def sortBy[K](f: (T) => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDDAPI[T] =
    create(data.sortBy(f,ascending,numPartitions)(ord,ctag))

  override def intersection(other: RDDAPI[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaEagerImpl[T] => create(data.intersection(parallelize(rddScala.data)))
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(data.intersection(rddSpark.data))
  }

  override def intersection(other: RDDAPI[T], numPartitions: Int): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaEagerImpl[T] => create(data.intersection(parallelize(rddScala.data), numPartitions))
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(data.intersection(rddSpark.data, numPartitions))
  }

  override def intersection(other: RDDAPI[T], partitioner: Partitioner)(implicit ord: Ordering[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaEagerImpl[T] => create(data.intersection(parallelize(rddScala.data), partitioner)(ord))
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(data.intersection(rddSpark.data, partitioner)(ord))
  }

  override def count(): Long =
    data.count()

  override def distinct(): RDDAPI[T] =
    create(data.distinct())

  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): RDDAPI[T] =
    create(data.distinct(numPartitions)(ord))

  override def top(num: Int)(implicit ord: Ordering[T]): Array[T] =
    data.top(num)(ord)

  override def subtract(other: RDDAPI[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaEagerImpl[T] => create(data.subtract(parallelize(rddScala.data)))
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(data.subtract(rddSpark.data))
  }

  override def subtract(other: RDDAPI[T], numPartitions: Int): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaEagerImpl[T] => create(data.subtract(parallelize(rddScala.data), numPartitions))
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(data.subtract(rddSpark.data, numPartitions))
  }

  override def subtract(other: RDDAPI[T], partitioner: Partitioner)(implicit ord: Ordering[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaEagerImpl[T] => create(data.subtract(parallelize(rddScala.data), partitioner))
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(data.subtract(rddSpark.data, partitioner))
  }

  override def cartesian[U: ClassTag](other: RDDAPI[U]): RDDAPI[(T, U)] = other match {
    case rddScala:RDDAPIScalaEagerImpl[U] => create(data.cartesian(parallelize(rddScala.data)))
    case rddSpark:RDDAPISparkImpl[U] => RDDAPI(data.cartesian(rddSpark.data))
  }

  override def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U =
    data.aggregate(zeroValue)(seqOp, combOp)

  override def groupBy[K](f: (T) => K)(implicit kt: ClassTag[K]): RDDAPI[(K, GenIterable[T])] =
    create(data.groupBy(f).mapValues(i => i.asInstanceOf[GenIterable[T]]))

  override def groupBy[K](f: (T) => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDDAPI[(K, GenIterable[T])] =
    create(data.groupBy(f, numPartitions).mapValues(i => i.asInstanceOf[GenIterable[T]]))

  override def groupBy[K](f: (T) => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K]): RDDAPI[(K, GenIterable[T])] =
    create(data.groupBy(f, p).mapValues(i => i.asInstanceOf[GenIterable[T]]))

  override def coalesce(numPartitions: Int, shuffle: Boolean, partitionCoalescer: Option[PartitionCoalescer])(implicit ord: Ordering[T]): RDDAPI[T] =
    create(data.coalesce(numPartitions, shuffle, partitionCoalescer))

  override def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] =
    data.takeOrdered(num)

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDDAPI[T] =
    RDDAPI(data.sample(withReplacement, fraction, seed))

  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[T] =
    data.takeSample(withReplacement, num, seed)

  override def randomSplit(weights: Array[Double], seed: Long): Array[RDDAPI[T]] =
    data.randomSplit(weights, seed).map(rdd => RDDAPI(rdd))

  override def toDataSet(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(spark.createDataset(data))

}
