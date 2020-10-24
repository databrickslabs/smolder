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
package com.databricks.smolder

class HL7FileFormatSuite extends SmolderBaseTest {

  test("loading an hl7 message with full class name") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("com.databricks.smolder.HL7FileFormat")
      .load(file)

    assert(df.collect().size === 1)
  }

  test("loading an hl7 message with short data source name") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    assert(df.collect().size === 1)
  }

  test("loading an hl7 message, selecting only the message header") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    assert(df.select("message").distinct().count() === 1)
  }

  test("loading an hl7 message, selecting only the segments") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    assert(df.selectExpr("explode(segments)").count() === 3)
  }

  test("loading an hl7 message, selecting only the segments ids") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    assert(df.selectExpr("explode(segments.id)").count() === 3)
  }

  test("loading an hl7 message, selecting only the segment fields") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    assert(df.selectExpr("explode(segments.fields) as fields")
      .selectExpr("explode(fields)")
      .count() === 57)
  }
}
