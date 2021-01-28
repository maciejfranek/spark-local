package com.datawizards.sparklocal.dataset.io

import com.datawizards.dmg.dialects.{DecoratorDialect, Dialect}

object ModelDialects {
  val _CSVDialect: Dialect = CSVDialect
  val _ParquetDialect: Dialect = ParquetDialect
  val _AvroDialect: Dialect = AvroDialect
  val _JSONDialect: Dialect = JSONDialect
}

class DummyDialect extends DecoratorDialect(null) {
  override protected def decorate(dataModel: String): String = null
}

case object CSVDialect extends DummyDialect
case object ParquetDialect extends DummyDialect
case object AvroDialect extends DummyDialect
case object JSONDialect extends DummyDialect
