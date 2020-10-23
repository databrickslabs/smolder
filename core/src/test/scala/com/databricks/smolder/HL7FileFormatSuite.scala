package com.databricks.smolder

class HL7FileFormatSuite extends SmolderBaseTest {

  test("loading an hl7 message with full class name") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("com.databricks.smolder.HL7FileFormat")
      .load(file)

    assert(df.collect().size === 1)
  }

  test("loading an hl7 message with short data source name") {

    val file = testFile("single_record.hl7")

    val df = spark.read.format("hl7")
      .load(file)

    assert(df.collect().size === 1)
  }
}
