package de.hpi.isg.sodap.rdfind.util

import de.hpi.isg.sodap.rdfind.util.NullSensitiveOrdered$Test.TestClass
import org.junit.{Assert, Test}

/**
 * @author sebastian.kruse 
 * @since 05.05.2015
 */
class NullSensitiveOrdered$Test {

  @Test
  def testOrdering = {
    Assert.assertTrue(TestClass(null) <= TestClass(null))
    Assert.assertTrue(TestClass(null) >= TestClass(null))
    Assert.assertTrue(TestClass("a") > TestClass(null))
    Assert.assertTrue(TestClass(null) < TestClass("a"))
    Assert.assertTrue(TestClass("a") >= TestClass("a"))
    Assert.assertTrue(TestClass("a") <= TestClass("a"))
    Assert.assertTrue(TestClass("a") < TestClass("b"))
    Assert.assertTrue(TestClass("b") > TestClass("a"))
  }

}

object NullSensitiveOrdered$Test {

  case class TestClass(value: String) extends NullSensitiveOrdered[TestClass] {
    override def compare(that: TestClass): Int = compareNullSensitive(this.value, that.value)
  }

}