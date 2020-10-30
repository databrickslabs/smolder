package com.databricks.smolder

import org.scalatest.FunSuite

class SegmentSuite extends FunSuite {

  test("cannot parse empty segment") {
    intercept[IllegalArgumentException] {
      Segment("")
    }
  }

  test("cannot parse segment that only contains an ID") {
    intercept[IllegalArgumentException] {
      Segment("PD1")
    }
  }

  test("properly parse a simple message") {
    val id = "PD1"
    val fields = Seq("",
      "",
      "d40726da-9b7a-49eb-9eeb-e406708bbb60",
      "",
      "Heller^Keneth")

    val segment = Segment((Seq(id) ++ fields).mkString("|"))

    assert(segment.id.toString === id)
    assert(segment.fields.size === fields.size)
    segment.fields
      .zip(fields)
      .foreach(p => {
        assert(p._1.toString === p._2)
      })
  }
}
