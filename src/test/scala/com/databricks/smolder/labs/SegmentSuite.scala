/*
 * Copyright 2020 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.labs.smolder

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
