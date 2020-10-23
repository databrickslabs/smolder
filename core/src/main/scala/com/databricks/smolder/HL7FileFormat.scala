package com.databricks.smolder

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.{ArrayData, CompressionCodecs}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConversions._
import scala.io.Source

object SerializedConfiguration {

  /**
    * Creates a serializable configuration from a Hadoop Configuration.
    * 
    * @param conf The configuration to serialize.
    * @return Returns a serializable configuration.
    */
  def apply(conf: Configuration): SerializedConfiguration = {
    val items = conf.iterator()
      .map(e => {
      (e.getKey, e.getValue)
      }).toSeq

    val kv = new Array[(String, String)](items.length)
    kv.indices.foreach(idx => {
      kv(idx) = items(idx)
    })

    SerializedConfiguration(kv.toSeq)
  }
}

/**
  * Serializable variant of the Hadoop Configuration.
  * 
  * Used for sharing config values (e.g., for file system initialization)
  * to Spark tasks.
  * 
  * @param kv Seq of key, value configuration pairs.
  */
case class SerializedConfiguration(kv: Seq[(String, String)]) {

  /**
   * @return Converts back to the original Hadoop Configuration.
   */
  def toHadoopConfiguration: Configuration = {
    val conf = new Configuration()
    kv.foreach(kv => {
      conf.set(kv._1, kv._2)
    })
    
    conf
  }
}

/**
 * File format for reading pipe delimited HL7 messages into a Spark DataFrame.
 */
class HL7FileFormat extends FileFormat with DataSourceRegister {
  
  override def shortName(): String = "hl7"

  // No write support yet.
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    ???
  }

  /* provides a fixed schema:
   * root
   *  |-- message: string (nullable = true)
   *  |-- segments: array (nullable = true)
   *  |    |-- element: struct (containsNull = true)
   *  |    |    |-- id: string (nullable = true)
   *  |    |    |-- fields: array (nullable = true)
   *  |    |    |    |-- element: string (containsNull = true)
   */
  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    Some(
      StructType(
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
      )
  }

  override def buildReader(
      spark: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    // serialize the hadoop configuration
    val serializableConf = SerializedConfiguration(hadoopConf)
    
    partitionedFile => {
      val path = new Path(partitionedFile.filePath)
      val hadoopFs = path.getFileSystem(serializableConf.toHadoopConfiguration)

      // open file, read fully, and close
      // if you don't close the handle, you will get timeouts on various cloud stores (e.g., s3)
      val is = hadoopFs.open(path)
      val lines = Source.fromInputStream(is).getLines().toArray
      is.close()

      // create HL7 iterator to parse message and return rows
      HL7Iterator(lines.toIterator, requiredSchema)
    }
  }
}

/**
  * Convenience class for parsing HL7 segments into Spark SQL Rows.
  * 
  * @param id The ID for this segment type (e.g., "PID" for Patient ID segment)
  * @param fields The pipe-separated fields within this HL7 segment.
  */
case class Segment(id: UTF8String,
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

object Message {

  /**
    * Parses HL7 messages from an iterator over strings.
    * 
    * @param lines An iterator containing all the lines from an HL7 message.
    * @return Parses the message into a Message case class.
    */
  def apply(lines: Iterator[String]): Message = {
    Message(UTF8String.fromString(lines.next),
            lines.toSeq
              .map(line => {
                // assume pipe delimited
                val fields = line.split('|').map(UTF8String.fromString(_))
                Segment(fields.head, fields.tail)
              }))
    
  }
}

/**
  * Convenience class for parsing HL7 messages into Spark SQL Rows.
  * 
  * @param message The message segment header text.
  * @param segments The segments contained within this message.
  */
case class Message(message: UTF8String,
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

case class HL7Iterator(
  lines: Iterator[String],
  requiredSchema: StructType)
    extends Iterator[InternalRow] {

  override def hasNext: Boolean = {
    lines.hasNext
  }

  override def next(): InternalRow = {
    // parse the rows in this file entirely
    val ir = Message(lines).toInternalRow(requiredSchema)

    ir
  }
}
