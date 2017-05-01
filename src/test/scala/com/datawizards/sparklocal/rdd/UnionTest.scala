package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UnionTest extends SparkLocalBaseTest {

  test("Union result") {
    assertRDDOperationResult(
      RDDAPI(Seq(1,2,3)) union RDDAPI(Seq(4,5))
    ) {
      Array(1,2,3,4,5)
    }
  }

  test("Union equal - scala union spark") {
    val r2 = RDDAPI(Seq(1,2,3))
    assertRDDOperationReturnsSameResult(Seq(4,5)){
      ds => ds union r2
    }
  }

  test("Union equal - spark union scala") {
    val r2 = RDDAPI(Seq(1,2,3))
    assertRDDOperationReturnsSameResult(Seq(4,5)){
      ds => r2 union ds
    }
  }

}