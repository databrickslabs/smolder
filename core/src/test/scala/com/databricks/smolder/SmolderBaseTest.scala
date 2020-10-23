package com.databricks.smolder

import java.net.URL
import org.apache.spark.DebugFilesystem
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.concurrent.Eventually

abstract class SmolderBaseTest
    extends FunSuite with Eventually with BeforeAndAfterEach {

  protected implicit def spark: SparkSession = {
    val session = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    SparkSession.setActiveSession(session)

    session
  }

  override def afterEach(): Unit = {
    eventually {
      DebugFilesystem.assertNoOpenStreams()
      assert(spark.sparkContext.getPersistentRDDs.isEmpty)
      assert(spark.sharedState.cacheManager.isEmpty, "Cache not empty.")
    }
  }

  /**
   * Finds the full path of a "test file," usually in the src/test/resources directory.
   *
   * @param name The path of the file w/r/t src/test/resources
   * @return The absolute path of the file
   * @throws IllegalArgumentException if the file doesn't exist
   */
  def testFile(name: String): String = {
    val url = resourceUrl(name)
    if (url == null) {
      throw new IllegalArgumentException("Couldn't find resource \"%s\"".format(name))
    }
    url.getFile
  }

  /**
   * Finds the URL of a "test file," usually in the src/test/resources directory.
   *
   * @param path The path of the file inside src/test/resources
   * @return The URL of the file
   */
  def resourceUrl(path: String): URL = {
    ClassLoader.getSystemClassLoader.getResource(path)
  }
}
