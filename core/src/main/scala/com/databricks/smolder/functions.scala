package com.databricks.smolder

import com.databricks.smolder.sql._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object functions {

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
