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

private[smolder] object Message {

  /**
   * @return The Spark SQL schema for an HL7 message.
   */
  def schema: StructType = StructType(
        Seq(
          StructField("message", StringType),
          StructField("segments",
            ArrayType(
              StructType(
                Seq(
                  StructField("id", StringType),
                  StructField("fields", ArrayType(StringType, false))
                )
              ), false)
            )
          )
        )

  /**
    * Parses HL7 messages from an iterator over strings.
    * 
    * @param lines An iterator containing all the lines from an HL7 message.
    * @return Parses the message into a Message case class.
    */
  def apply(lines: Iterator[String]): Message = {
    require(lines.hasNext, "Received empty message.")

    Message(UTF8String.fromString(lines.next),
            lines.toSeq
              .map(Segment(_)))
  }

  /**
    * Parses HL7 messages from a string.
    *
    * Returns a null if the input is null.
    * 
    * @param text A string to parse.
    * @return Parses the message into a Message case class.
    */
  def apply(text: UTF8String): Message = {
    if (text == null) {
      null
    } else {
      val textString = text.toString
      require(textString.nonEmpty, "Received empty string.")

      Message(textString.split('\n').toIterator)
    }
  }
}

/**
  * Convenience class for parsing HL7 messages into Spark SQL Rows.
  * 
  * @param message The message segment header text.
  * @param segments The segments contained within this message.
  */
private[smolder] case class Message(message: UTF8String,
  segments: Seq[Segment]) {

  /**
   * Returns a message as a row, with all possible fields included.
   * 
   * @return Converts into a Spark SQL InternalRow.
   */
  def toInternalRow(): InternalRow = {
    toInternalRow(Message.schema)
  }

  /**
   * Returns a message as a row, possibly with some fields projected away.
   *  
   * @param requiredSchema The schema to project.
   * @return Converts into a Spark SQL InternalRow.
   */
  def toInternalRow(requiredSchema: StructType): InternalRow = {
    def makeSegments: ArrayData = {
      ArrayData.toArrayData(segments.map(s => {
        s.toInternalRow(requiredSchema("segments")
          .dataType
          .asInstanceOf[ArrayType]
          .elementType
          .asInstanceOf[StructType])
      }).toArray)
    }
    val fieldNames = requiredSchema.fieldNames
    val messageFields = (fieldNames.contains("message"), fieldNames.contains("segments"))

    messageFields match {
      case (true, true) => InternalRow(message, makeSegments)
      case (true, false) => InternalRow(message)
      case (false, true) => InternalRow(makeSegments)
      case (_, _) => InternalRow()
    }
  }
}

