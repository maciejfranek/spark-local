package com.datawizards.sparklocal

import com.datawizards.dmg.annotations.{column, table}
import com.datawizards.dmg.dialects
import com.datawizards.sparklocal.dataset.io.ModelDialects

object TestModel {
  case class Person(name: String, age: Int)
  case class PersonV2(name: String, age: Int, title: Option[String])
  case class PersonV3(name: String, age: Int, title: Option[String], salary: Option[Long])
  case class PersonBigInt(name: String, age: BigInt)
  case class PersonUppercase(NAME: String, AGE: Int)
  @table("PEOPLE", dialect = dialects.HiveDialect)
  case class PersonWithMapping(
    @column("PERSON_NAME_HIVE", dialect = dialects.HiveDialect)
    @column("PERSON_NAME_H2", dialect = dialects.H2Dialect)
    @column("PERSON_NAME_CSV", dialect = ModelDialects._CSVDialect)
    @column("personNameJson", dialect = ModelDialects._JSONDialect)
    @column("personNameAvro", dialect = ModelDialects._AvroDialect)
    @column("personNameParquet", dialect = ModelDialects._ParquetDialect)
    name: String,
    @column("PERSON_AGE_HIVE", dialect = dialects.HiveDialect)
    @column("PERSON_AGE_H2", dialect = dialects.H2Dialect)
    @column("PERSON_AGE_CSV", dialect = ModelDialects._CSVDialect)
    @column("personAgeJson", dialect = ModelDialects._JSONDialect)
    @column("personAgeAvro", dialect = ModelDialects._AvroDialect)
    @column("personAgeParquet", dialect = ModelDialects._ParquetDialect)
    age: Int
  )
  @table("PEOPLE", dialect = dialects.HiveDialect)
  case class PersonWithMappingV2(
                                @column("PERSON_NAME_HIVE", dialect = dialects.HiveDialect)
                                @column("PERSON_NAME_H2", dialect = dialects.H2Dialect)
                                @column("PERSON_NAME_CSV", dialect = ModelDialects._CSVDialect)
                                @column("personNameJson", dialect = ModelDialects._JSONDialect)
                                @column("personNameAvro", dialect = ModelDialects._AvroDialect)
                                @column("personNameParquet", dialect = ModelDialects._ParquetDialect)
                                name: String,
                                @column("PERSON_AGE_HIVE", dialect = dialects.HiveDialect)
                                @column("PERSON_AGE_H2", dialect = dialects.H2Dialect)
                                @column("PERSON_AGE_CSV", dialect = ModelDialects._CSVDialect)
                                @column("personAgeJson", dialect = ModelDialects._JSONDialect)
                                @column("personAgeAvro", dialect = ModelDialects._AvroDialect)
                                @column("personAgeParquet", dialect = ModelDialects._ParquetDialect)
                                age: Int,
                                @column("PERSON_TITLE_HIVE", dialect = dialects.HiveDialect)
                                @column("PERSON_TITLE_H2", dialect = dialects.H2Dialect)
                                @column("PERSON_TITLE_CSV", dialect = ModelDialects._CSVDialect)
                                @column("personTitleJson", dialect = ModelDialects._JSONDialect)
                                @column("personTitleAvro", dialect = ModelDialects._AvroDialect)
                                @column("personTitleParquet", dialect = ModelDialects._ParquetDialect)
                                title: Option[String]
                              )
  @table("PEOPLE", dialect = dialects.HiveDialect)
  case class PersonWithMappingV3(
                                  @column("PERSON_NAME_HIVE", dialect = dialects.HiveDialect)
                                  @column("PERSON_NAME_H2", dialect = dialects.H2Dialect)
                                  @column("PERSON_NAME_CSV", dialect = ModelDialects._CSVDialect)
                                  @column("personNameJson", dialect = ModelDialects._JSONDialect)
                                  @column("personNameAvro", dialect = ModelDialects._AvroDialect)
                                  @column("personNameParquet", dialect = ModelDialects._ParquetDialect)
                                  name: String,
                                  @column("PERSON_AGE_HIVE", dialect = dialects.HiveDialect)
                                  @column("PERSON_AGE_H2", dialect = dialects.H2Dialect)
                                  @column("PERSON_AGE_CSV", dialect = ModelDialects._CSVDialect)
                                  @column("personAgeJson", dialect = ModelDialects._JSONDialect)
                                  @column("personAgeAvro", dialect = ModelDialects._AvroDialect)
                                  @column("personAgeParquet", dialect = ModelDialects._ParquetDialect)
                                  age: Int,
                                  @column("PERSON_TITLE_HIVE", dialect = dialects.HiveDialect)
                                  @column("PERSON_TITLE_H2", dialect = dialects.H2Dialect)
                                  @column("PERSON_TITLE_CSV", dialect = ModelDialects._CSVDialect)
                                  @column("personTitleJson", dialect = ModelDialects._JSONDialect)
                                  @column("personTitleAvro", dialect = ModelDialects._AvroDialect)
                                  @column("personTitleParquet", dialect = ModelDialects._ParquetDialect)
                                  title: Option[String],
                                  @column("PERSON_SALARY_HIVE", dialect = dialects.HiveDialect)
                                  @column("PERSON_SALARY_H2", dialect = dialects.H2Dialect)
                                  @column("PERSON_SALARY_CSV", dialect = ModelDialects._CSVDialect)
                                  @column("personSalaryJson", dialect = ModelDialects._JSONDialect)
                                  @column("personSalaryAvro", dialect = ModelDialects._AvroDialect)
                                  @column("personSalaryParquet", dialect = ModelDialects._ParquetDialect)
                                  salary: Option[Long]
                                )

  case class Book(title: String, year: Int, personName: String)
  case class LargeClass(strVal   : String,
                        intVal   : Int,
                        longVal  : Long,
                        doubleVal: Double,
                        floatVal : Float,
                        shortVal : Short,
                        flag     : Boolean,
                        byteVal  : Byte)

  implicit val peopleOrdering = new Ordering[Person]() {
    override def compare(x: Person, y: Person): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val peopleBigIntOrdering = new Ordering[PersonBigInt]() {
    override def compare(x: PersonBigInt, y: PersonBigInt): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val peopleV3Ordering = new Ordering[PersonV3]() {
    override def compare(x: PersonV3, y: PersonV3): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val peopleMappingOrdering = new Ordering[PersonWithMapping]() {
    override def compare(x: PersonWithMapping, y: PersonWithMapping): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val peopleMappingV3Ordering = new Ordering[PersonWithMappingV3]() {
    override def compare(x: PersonWithMappingV3, y: PersonWithMappingV3): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val booksOrdering = new Ordering[Book]() {
    override def compare(x: Book, y: Book): Int =
      if(x == null) -1 else if(y == null) 1 else x.title.compareTo(y.title)
  }
}
