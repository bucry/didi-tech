package com.didi.train

import com.didi.models.{OrderFeatureTable, Traffic}
import org.scalatest.{FunSuite, Matchers}


class FeatureExtraction$Test extends FunSuite with Matchers {

  test("testExtract") {
     FeatureExtraction.extract(Traffic("NULL", "NULL", "NULL")) shouldEqual OrderFeatureTable()
  }

}
