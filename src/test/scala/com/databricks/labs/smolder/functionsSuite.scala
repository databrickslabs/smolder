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

import com.databricks.labs.smolder._
import org.apache.spark.sql.functions._

case class TextFile(file: String, value: String)

class functionsSuite extends SmolderBaseTest {

  test("loading an hl7 message as text and then parse") {

    val file = testFile("single_record.hl7")

    val df = spark.createDataFrame(spark.sparkContext
      .wholeTextFiles(file)
      .map(p => TextFile(p._1, p._2)))

    val cleanDF  = df.select(regexp_replace(df("value"), "\n", "\r").alias("clean"))

    val f = new functions(spark.sqlContext, cleanDF)
    val hl7Df = cleanDF.select(f.parse_hl7_message(cleanDF("clean")).alias("hl7"))

    assert(hl7Df.count() === 1)
    assert(hl7Df.selectExpr("explode(hl7.segments)").count() === 3)
    assert(hl7Df.selectExpr("explode(hl7.segments) as segments")
      .selectExpr("explode(segments.fields)")
      .count() === 57)
  }

  test("use the segment field function to extract the event type") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    val f = new functions(spark.sqlContext, df)
    val evnType = df.select(f.segment_field("EVN", 0).alias("type"))
    assert(evnType.count() === 1)
    assert(evnType.first().getString(0) === "A03")
  }

  test("use the segment field function to extract the event type, different column name") {

    val file = testFile("single_record.hl7")

    val df = spark.createDataFrame(spark.sparkContext
      .wholeTextFiles(file)
      .map(p => TextFile(p._1, p._2)))

    val cleanDF  = df.select(regexp_replace(df("value"), "\n", "\r").alias("clean"))

    val f = new functions(spark.sqlContext, cleanDF)

    val hl7Df = cleanDF.select(f.parse_hl7_message(cleanDF("clean")).alias("hl7"))

    val evnType = hl7Df.select(f.segment_field("EVN", 0, col("hl7.segments"))
      .alias("type"))
    assert(evnType.count() === 1)
    assert(evnType.first().getString(0) === "A03")
  }

  test("use the segment field and subfield functions to extract the patient's first name") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    val f = new functions(spark.sqlContext, df)

    val pidName = df.select(f.segment_field("PID", 4).alias("name"))
    assert(pidName.count() === 1)
    assert(pidName.first().getString(0) === "Heller^Keneth")

    val firstName = pidName.select(f.subfield(col("name"), 1))
    assert(firstName.count() === 1)
    assert(firstName.first().getString(0) === "Keneth")
  }
}
