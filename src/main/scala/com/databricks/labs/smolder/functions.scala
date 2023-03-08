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

import com.databricks.labs.smolder.sql._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object functions {

  /**
   * Extracts the value at a specific index in a repeating field
   * 
   * @param col A column containing the repeated field from a message segment
   * @param repIndex The index of repeated field value that must be extracted
   * @return Yields a new column containing the field of a message segment.
   */
  def repeating_field(col: Column, repIndex: Int, delim: String="~"): Column = {
    split(col, delim).getItem(repIndex)
  }

  /**
    * Parses a textual, pipe-delimited HL7v2 message.
    * 
    * @param message Column containing HL7v2 message text to parse.
    * @return New column containing parsed HL7 message structure.
    */
  def parse_hl7_message(message: Column): Column = {
    new Column(new ParseHL7Message(message.expr))
  }

  /**
   * Extracts a field from a message segment.
   * 
   * @param segment The ID of the segment to extract.
   * @param field The index of the field to extract.
   * @param segmentColumn The name of the column containing message segments.
   *   Defaults to "segments".
   * @return Yields a new column containing the field of a message segment.
   * 
   * @note If there are multiple segments with the same ID, this function will
   *   select the field from one of the segments. Order is undefined.
   */
  def segment_field(segment: String,
    field: Int,
    segmentColumn: Column = col("segments")): Column = {

    filter(segmentColumn, s => s("id") === lit(segment))
      .getItem(0)
      .getField("fields")
      .getItem(field)
  }

  /**
   * Parses a "^" delimited subfield from a selected segment field.
   * 
   * @param col The segment field to parse.
   * @param subfield The index of the subfield to return.
   * @return Returns the value at this location in the subfield.
   */
  def subfield(col: Column, subfield: Int): Column = {
    split(col, "\\^").getItem(subfield)
  }
}
