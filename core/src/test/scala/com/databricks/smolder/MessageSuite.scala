package com.databricks.smolder

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.io.Source

class MessageSuite extends SmolderBaseTest {

  val msh = "MSH|^~\\&|||||20201020150800.739+0000||ADT^A03^ADT_A03|11374301|P|2.4"

  test("validate schema") {
    val schema = Message.schema

    assert(schema.size === 2)
    assert(schema("message").dataType === StringType)
    assert(schema("segments").dataType match {
      case ArrayType(_, false) => true
      case _ => false
    })

    val segmentSchema = schema("segments").dataType match {
      case ArrayType(structType: StructType, _) => structType
    }
    assert(segmentSchema.size === 2)
    assert(segmentSchema("id").dataType === StringType)
    assert(segmentSchema("fields").dataType match {
      case ArrayType(StringType, false) => true
      case _ => false
    })
  }

  test("cannot parse an empty iterator") {
    intercept[IllegalArgumentException] {
      Message(Iterator())
    }
  }

  test("cannot parse an empty string") {
    intercept[IllegalArgumentException] {
      Message(UTF8String.fromString(""))
    }
  }

  test("passing a null string returns a null message") {
    val nullMessage: UTF8String = null
    assert(Message(nullMessage) === null)
  }

  test("parse only a message header, by iterator") {

    val message = Message(Iterator(msh))

    assert(message.message.toString === msh)
    assert(message.segments.isEmpty)
  }

  test("parse only a message header, by string") {

    val message = Message(UTF8String.fromString(msh))

    assert(message.message.toString === msh)
    assert(message.segments.isEmpty)
  }

  test("parse a full message, by iterator") {

    val file = testFile("single_record.hl7")
    val lines = Source.fromFile(file).getLines()

    val message = Message(lines)

    assert(message.message.toString === msh)

    val segments = message.segments
    assert(segments.size === 3)

    def validateSegment(idx: Int, id: String, size: Int) {
      assert(segments(idx).id.toString === id)
      assert(segments(idx).fields.size === size)
    }

    validateSegment(0, "EVN", 2)
    validateSegment(1, "PID", 11)
    validateSegment(2, "PV1", 44)
  }

  test("parse a full message, by string") {

    val file = testFile("single_record.hl7")
    val lines = Source.fromFile(file).getLines().mkString("\n")

    val message = Message(UTF8String.fromString(lines))

    assert(message.message.toString === msh)

    val segments = message.segments
    assert(segments.size === 3)

    def validateSegment(idx: Int, id: String, size: Int) {
      assert(segments(idx).id.toString === id)
      assert(segments(idx).fields.size === size)
    }

    validateSegment(0, "EVN", 2)
    validateSegment(1, "PID", 11)
    validateSegment(2, "PV1", 44)
  }
}
