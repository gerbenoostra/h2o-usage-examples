package org.gerbenoostra.h2o.example.train

import com.typesafe.scalalogging.LazyLogging
import hex.genmodel.utils.DistributionFamily
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.gbm.{GBM, GBMModel}
import hex.{Model, ModelMetricsBinomial}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import water.fvec.{Frame, H2OFrame}
import water.support.{H2OFrameSupport, ModelMetricsSupport}

object ModelPipeline extends LazyLogging {

  def train(spark: SparkSession, h2OContext: H2OContext, table: H2OFrame, target: String): Model[_, _, _] = {
    implicit val sqlContext: SQLContext = spark.sqlContext
    implicit val hc: H2OContext = h2OContext
    import hc.implicits._

    // Split table
    val (train: Frame, valid: Frame) = splitDataSet(table)


    logger.info("====== Build model ======")
    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._ignored_columns = Array("ID")
    gbmParams._response_column = target
    gbmParams._ntrees = 5
    gbmParams._valid = valid
    gbmParams._nfolds = 3
    gbmParams._min_rows = 1
    gbmParams._distribution = DistributionFamily.multinomial

    val gbm = new GBM(gbmParams)
    val gbmModel = gbm.trainModel.get

    logger.info("====== Train Metrics ======")
    val trainMetrics: ModelMetricsBinomial = evaluate(train, gbmModel)
    val confusionMatrix = trainMetrics.cm()
    logger.info(s"Train f1 = ${confusionMatrix.f1()}")
    logger.info("====== Validation Metrics ======")
    val validMetrics: ModelMetricsBinomial = evaluate(valid, gbmModel)

    logger.info("====== Validation Input ======")
    h2OContext.asDataFrame(valid).show()
    logger.info("====== Validation Predictions ======")
    val fr = gbmModel.score(valid)
    h2OContext.asDataFrame(fr).show()

    gbmModel
  }

  private def splitDataSet(table: H2OFrame) = {
    logger.info("====== Split data set ======")
    val keys = Array[String]("train.hex", "valid.hex", "test.hex")
    val ratios = Array[Double](0.7, 0.2)
    val frs = H2OFrameSupport.split(table, keys, ratios)
    val (train, valid, test) = (frs(0), frs(1), frs(2))
    table.delete()
    (train, valid)
  }

  private def evaluate(train: Frame, gbmModel: GBMModel) = {
    logger.info("====== Train Metrics ======")
    val trainMetrics = ModelMetricsSupport.modelMetrics[ModelMetricsBinomial](gbmModel, train)
    logger.info(trainMetrics.toString)
    trainMetrics
  }

}
