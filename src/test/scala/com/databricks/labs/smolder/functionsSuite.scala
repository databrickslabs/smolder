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

import com.databricks.labs.smolder.functions._
import org.apache.spark.sql.functions._

case class TextFile(file: String, value: String)

class functionsSuite extends SmolderBaseTest {

  test("loading an hl7 message as text and then parse") {

    val file = testFile("single_record.hl7")

    val df = spark.createDataFrame(spark.sparkContext
      .wholeTextFiles(file)
      .map(p => TextFile(p._1, p._2)))

    val cleanDF  = df.select(regexp_replace(df("value"), "\n", "\r").alias("clean"))

    val hl7Df = cleanDF.select(parse_hl7_message(cleanDF("clean")).alias("hl7"))

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

    val evnType = df.select(segment_field("EVN", 0).alias("type"))
    assert(evnType.count() === 1)
    assert(evnType.first().getString(0) === "A03")
  }

  test("use the segment field function to extract the event type, different column name") {

    val file = testFile("single_record.hl7")

    val df = spark.createDataFrame(spark.sparkContext
      .wholeTextFiles(file)
      .map(p => TextFile(p._1, p._2)))

    val cleanDF  = df.select(regexp_replace(df("value"), "\n", "\r").alias("clean"))
    val hl7Df = cleanDF.select(parse_hl7_message(cleanDF("clean")).alias("hl7"))

    val evnType = hl7Df.select(segment_field("EVN", 0, col("hl7.segments"))
      .alias("type"))
    assert(evnType.count() === 1)
    assert(evnType.first().getString(0) === "A03")
  }

  test("Test repeating segment function"){
    //get a "stable identifier" i.e. val for import
    val spark2 = spark
    import spark2.implicits._

    val df = Seq("MSH\rTST^1234567890^^^HOSPITALONE^MRN~4646464646^^^HOSPITALTWO^MRN~9431675613^^^HOSPITALTHRE^MRN").toDF("text")
    val hl7Df = df.select(parse_hl7_message(df("text")).alias("hl7"))
    assert( hl7Df.select(segment_field("TST", 0, col("hl7.segments")).alias("TST_0")).select(repeating_field(col("TST_0"), 0, "~")).first().getString(0) === "1234567890^^^HOSPITALONE^MRN" )
    assert( hl7Df.select(segment_field("TST", 0, col("hl7.segments")).alias("TST_0")).select(repeating_field(col("TST_0"), 1, "~")).first().getString(0) === "4646464646^^^HOSPITALTWO^MRN" )
    assert( hl7Df.select(segment_field("TST", 0, col("hl7.segments")).alias("TST_0")).select(repeating_field(col("TST_0"), 2, "~")).first().getString(0) === "9431675613^^^HOSPITALTHRE^MRN" )

  }

  test("use the segment field and subfield functions to extract the patient's first name") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    val pidName = df.select(segment_field("PID", 4).alias("name"))
    assert(pidName.count() === 1)
    assert(pidName.first().getString(0) === "Heller^Keneth")

    val firstName = pidName.select(subfield(col("name"), 1))
    assert(firstName.count() === 1)
    assert(firstName.first().getString(0) === "Keneth")
  }
}
