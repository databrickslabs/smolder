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

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConversions._

private[smolder] object Segment {

  /**
   * Parses a segment from an HL7 message.
   * 
   * @param segment The message segment to parse.
   * @return Returns the message parsed into a Segment model.
   */
  def apply(segment: String): Segment = {

    require(segment.nonEmpty, "Received empty segment.")

    // assume pipe delimited
    val fields = segment.split('|').map(UTF8String.fromString(_))

    require(fields.size > 1,
      "Encountered message segment with insufficient fields: %s".format(segment))

    Segment(fields.head, fields.tail)
  }
}

/**
  * Convenience class for parsing HL7 segments into Spark SQL Rows.
  * 
  * @param id The ID for this segment type (e.g., "PID" for Patient ID segment)
  * @param fields The pipe-separated fields within this HL7 segment.
  */
private[smolder] case class Segment(id: UTF8String,
  fields: Seq[UTF8String]) {

  /**
    * @return Converts into a Spark SQL InternalRow.
    */
  def toInternalRow(requiredSchema: StructType): InternalRow = {
    val fieldNames = requiredSchema.fieldNames
    val segmentFields = (fieldNames.contains("id"), fieldNames.contains("fields"))

    segmentFields match {
      case (true, true) => InternalRow(id, ArrayData.toArrayData(fields.toArray))
      case (true, false) => InternalRow(id)
      case (false, true) => InternalRow(ArrayData.toArrayData(fields.toArray))
      case (_, _) => InternalRow()
    }
  }
}
