package org.gerbenoostra.testing.sparkling_water

import org.apache.spark.SparkConf
import org.apache.spark.h2o.H2OContext
import org.gerbenoostra.testing.spark.SharedSparkContext
import org.scalatest.Suite

/**
  * Shares a local `SparkContext` between all tests in a suite
  * and closes it at the end. You can share between suites by enabling
  * reuseContextIfPossible.
  */
trait SharedH2OContext extends SharedSparkContext{
  self: Suite =>

  @transient private var _h2o: H2OContext = _

  /**
    * The configured h2o context, as def to provide to the base test setup and teardown
    *
    * @return configured h2o context
    */
  def h2oContext: H2OContext = _h2o


  /**
    * Setup to run once before all tests in the integration test suite
    */
  override def beforeAll() {
    super.beforeAll()
    _h2o = H2OContext.getOrCreate(spark)
  }

  /**
    * Tear down the spark session once after all tests
    * Will do nothing if <code>reuseContextIfPossible</code> is true
    */
  override def afterAll() {
    if (!reuseContextIfPossible) {
      _h2o.stop(false)
    }
    super.afterAll()
  }

  override def conf: SparkConf = {
    super.conf
      .set("spark.locality.wait", "30500")
      .set("spark.ext.h2o.stacktrace.collector.interval", "-1")
  }
}
