package org.gerbenoostra.h2o.example.train

import java.io.File

import hex.Model

object OriginalModelExporter {

  def exportModel(model: Model[_, _, _], file: File): Unit = {
    import water.support.ModelSerializationSupport._
    exportMOJOModel(model, file.getPath, true)
  }

}
