package com.databricks.smolder

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

