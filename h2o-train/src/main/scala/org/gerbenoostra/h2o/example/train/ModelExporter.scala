package org.gerbenoostra.h2o.example.train

import java.io.File

import hex.Model

object ModelExporter {

  def exportModel(model: Model[_, _, _], file: File): Unit = {
    import java.io.FileOutputStream
    val outputStream = new FileOutputStream(file)
    try {
      model.getMojo.writeTo(outputStream)
    }
    finally if (outputStream != null) outputStream.close()
  }
}
