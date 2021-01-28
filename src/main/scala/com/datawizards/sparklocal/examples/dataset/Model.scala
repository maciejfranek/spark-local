package com.datawizards.sparklocal.examples.dataset

import com.datawizards.dmg.annotations.{column, table}
import com.datawizards.dmg.dialects
import com.datawizards.sparklocal.dataset.io.ModelDialects

object Model {
  case class Person(id: Int, name: String, gender: String)
  case class WorkExperience(personId: Int, year: Int, title: String)
  case class HRReport(year: Int, title: String, gender: String, count: Int)
  @table("PEOPLE", dialect = dialects.HiveDialect)
  case class PersonWithMapping(
                                @column("PERSON_NAME", dialect = dialects.HiveDialect)
                                @column("PERSON_NAME", dialect = ModelDialects._CSVDialect)
                                @column("personName", dialect = ModelDialects._JSONDialect)
                                @column("personName", dialect = ModelDialects._AvroDialect)
                                @column("personName", dialect = ModelDialects._ParquetDialect)
                                name: String,
                                @column("PERSON_AGE", dialect = dialects.HiveDialect)
                                @column("PERSON_AGE", dialect = ModelDialects._CSVDialect)
                                @column("personAge", dialect = ModelDialects._JSONDialect)
                                @column("personAge", dialect = ModelDialects._AvroDialect)
                                @column("personAge", dialect = ModelDialects._ParquetDialect)
                                age: Int
                              )
}