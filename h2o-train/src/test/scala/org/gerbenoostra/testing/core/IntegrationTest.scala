package org.gerbenoostra.testing.core

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfter, DiagrammedAssertions, Matchers, WordSpec}

trait IntegrationTest extends WordSpec
  with DiagrammedAssertions
  with Matchers
  with BeforeAndAfter
  with LazyLogging{

}
