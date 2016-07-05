package de.hpi.isg.sodap.rdfind.util

import org.junit.Assert._
import org.junit.{Assert, Test}

class ConditionCodes$Test {

  def test: Unit = ???

  @Test
  def testIsBinaryCondition = {
    List(9, 10, 12, 17, 18, 20, 33, 34, 36).foreach { code =>
      Assert.assertFalse(s"Code $code was classified incorrectly!", ConditionCodes.isBinaryCondition(code))
    }
    List(11, 13, 14, 19, 21, 22, 35, 37, 38).foreach { code =>
      Assert.assertTrue(s"Code $code was classified incorrectly!", ConditionCodes.isBinaryCondition(code))
    }
  }
  @Test
  def testIsUnaryCondition = {
    List(9, 10, 12, 17, 18, 20, 33, 34, 36).foreach { code =>
      Assert.assertTrue(s"Code $code was classified incorrectly!", ConditionCodes.isUnaryCondition(code))
    }
    List(11, 13, 14, 19, 21, 22, 35, 37, 38).foreach { code =>
      Assert.assertFalse(s"Code $code was classified incorrectly!", ConditionCodes.isUnaryCondition(code))
    }
  }
  @Test
  def testSanityCheck = {
    val allCodes = List(10, 12, 17, 20, 33, 34) ++ List(14, 21, 35)
    for (i <- 0 to 255) {
      Assert.assertEquals(s"False sanity check on $i.", allCodes.contains(i), ConditionCodes.isValidStandardCapture(i))
    }
  }

}