package com.databricks.smolder

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import scala.collection.JavaConversions._
import scala.io.Source

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
    Some(Message.schema)
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
    val serializableConf = new SerializableConfiguration(hadoopConf)
    
    partitionedFile => {
      val path = new Path(partitionedFile.filePath)
      val hadoopFs = path.getFileSystem(serializableConf.value)

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

private case class HL7Iterator(
  lines: Iterator[String],
  requiredSchema: StructType)
    extends Iterator[InternalRow] {

  var accessed = false

  // parse the rows in this file entirely
  val ir = Message(lines).toInternalRow(requiredSchema)

  override def hasNext: Boolean = {
    !accessed
  }

  override def next(): InternalRow = {
    require(!accessed, "Called next on empty iterator.")
    accessed = true
    ir
  }
}
