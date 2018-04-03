package org.gerbenoostra.h2o.example.train

import java.io._
import java.nio.file.Path

import hex.Model
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession
import org.gerbenoostra.h2o.example.predict
import org.gerbenoostra.testing.core.IntegrationTest
import org.gerbenoostra.testing.sparkling_water.SharedH2OContext
import water.fvec.H2OFrame

class SparklingWaterTest extends IntegrationTest with SharedH2OContext {

  "Alternative serialization" should {
    "allow loading outside spark" in {
      val myspark = this.spark
      implicit val hc: H2OContext = this.h2oContext
      val gbmModel: Model[_, _, _] = setupModel(myspark)

      val path: Path = getTempMojoPath
      ModelExporter.exportModel(gbmModel, path.toFile)

      // using only h2o-genmodel package
      val easyPredictModelWrapper = predict.Main.loadModel(path.toFile)
      logger.info("==== MODEL class = " + easyPredictModelWrapper.m.getClass)

      testPrediction(easyPredictModelWrapper)
    }

  }

  "Original serialization" should {
    "allow loading outside spark" in {
      val myspark = this.spark
      implicit val hc: H2OContext = this.h2oContext
      val gbmModel: Model[_, _, _] = setupModel(myspark)

      val path: Path = getTempMojoPath
      OriginalModelExporter.exportModel(gbmModel, path.toFile)

      // using only h2o-genmodel package
      val easyPredictModelWrapper = predict.Main.loadModel(path.toFile)
      logger.info("==== MODEL class = " + easyPredictModelWrapper.m.getClass)

      testPrediction(easyPredictModelWrapper)
    }

  }

  private def setupModel(myspark: SparkSession) = {
    val table = new H2OFrame(new File("../examples/smalldata/prostate/prostate.csv"))

    val target = "CAPSULE"
    table.replace(table.find(target), table.vec(target).toCategoricalVec).remove()

    val gbmModel = ModelPipeline.train(myspark, h2oContext, table, target)
    gbmModel
  }

  private def getTempMojoPath = {
    val path = java.nio.file.Files.createTempFile("model", ".mojo")
    path.toFile.deleteOnExit()
    path
  }

  private def testPrediction(easyPredictModelWrapper: EasyPredictModelWrapper) = {
    val row = new RowData
    row.put("AGE", "50")
    row.put("RACE", "2")
    row.put("DPROS", "1")
    row.put("DCAPS", "2")
    row.put("PSA", "13")
    row.put("VOL", "0")
    row.put("GLEASON", "6")
    val p = easyPredictModelWrapper.predictBinomial(row)
    logger.info("Example row gets label:" + p.label)
    logger.info("Class probabilities: " + p.classProbabilities.mkString(", "))
  }

}
