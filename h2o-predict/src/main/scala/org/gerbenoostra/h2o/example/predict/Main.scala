package org.gerbenoostra.h2o.example.predict

import java.io.{File, FileInputStream}

import com.typesafe.scalalogging.LazyLogging
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import hex.genmodel.{ModelMojoReader, MojoReaderBackendFactory}


object Main extends App with LazyLogging {

  override def main(args: Array[String]): Unit = {
    val file = new File("./model.mojo")
    val model: EasyPredictModelWrapper = loadModel(file)
    val row = new RowData
    row.put("AGE", "68")
    row.put("RACE", "2")
    row.put("DCAPS", "2")
    row.put("VOL", "0")
    row.put("GLEASON", "6")
    val p = model.predictBinomial(row)
    println("Predicted label: " + p.label)
    println("Class probabilities: "+p.classProbabilities.mkString(", "))
  }

  def loadModel(file: File): EasyPredictModelWrapper = {
    val is = new FileInputStream(file)
    val model: EasyPredictModelWrapper = try {
      loadModel(is)
    } finally {
      is.close()
    }
    model
  }

  def loadModel(is: FileInputStream): EasyPredictModelWrapper = {
    val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    val mojoModel = ModelMojoReader.readFrom(reader)
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(mojoModel)
    config.setConvertUnknownCategoricalLevelsToNa(true)
    val model = new EasyPredictModelWrapper(config)
    model
  }
}