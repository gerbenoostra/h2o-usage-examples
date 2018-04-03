package org.gerbenoostra.testing.spark

import com.holdenkarau.spark.testing.{LocalSparkContext, SparkContextProvider}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{EvilSparkContext, SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Shares a local `SparkContext` between all tests in a suite
  * and closes it at the end. You can share between suites by enabling
  * reuseContextIfPossible.
  */
trait SharedSparkContext extends BeforeAndAfterAll with SparkContextProvider {
  self: Suite =>
  // We copied this from holdenkarau, and fixed it to be able to mix it with other traits having beforeAll

  @transient private var _sc: SparkContext = _

  @transient private var _spark: SparkSession = _

  /**
    * The configured spark context, as def to provide to the base test setup and teardown
    *
    * @return configured spark context
    */
  override def sc: SparkContext = _sc

  /**
    * Defines whether or not the context should be reused
    *
    * @return the current state, if false, will stop the spark session in the afterall
    */
  protected implicit def reuseContextIfPossible: Boolean = false


  /**
    * The spark session, only usable after a call to <code>beforeAll()</code>
    *
    * @return a running instance of spark, which knows how to connect to cassandra
    */
  def spark: SparkSession = _spark

  /**
    * Setup to run once before all tests in the integration test suite
    */
  override def beforeAll() {
    // This is kind of a hack, but if we've got an existing Spark Context
    // hanging around we need to kill it.
    if (!reuseContextIfPossible) {
      EvilSparkContext.stopActiveSparkContext()
    }
    _spark = SparkSession.builder().config(conf).getOrCreate()
    _sc = spark.sparkContext
    setup(_sc)
  }

  /**
    * Tear down the spark session once after all tests
    * Will do nothing if <code>reuseContextIfPossible</code> is true
    */
  override def afterAll() {
    if (!reuseContextIfPossible) {
      spark.stop()
      LocalSparkContext.stop(_sc)
      _sc = null
      _spark = null
    }
  }

  override def conf: SparkConf = {
    super.conf
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}
