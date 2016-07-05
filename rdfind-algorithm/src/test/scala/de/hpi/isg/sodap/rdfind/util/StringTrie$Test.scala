package de.hpi.isg.sodap.rdfind.util

import org.junit.Test
import org.junit.Assert._

/**
 * @author sebastian.kruse 
 * @since 18.06.2015
 */
class StringTrie$Test {

  @Test
  def testNormalAddingAndReading = {
    // Create a trie and add some test keys.
    val trie = new StringTrie[String]
    val testEntries = List("http://dbpedia.org/resource/" -> "dbpr",
      "http://dbpedia.org/resource/Category:" -> "dbpc",
      "http://xmlns.com/foaf/0.1/" -> "foaf",
      "urn:yahoo:maps" -> "y")
    testEntries.foreach(trie += _)

    // Test the entries directly.
    testEntries.foreach { entry =>
      assertEquals(entry._2, trie(entry._1))
    }

    // Test the entries with a suffix.
    testEntries.foreach { entry =>
      assertEquals(entry._2, trie(entry._1 + "~"))
      assertEquals(entry._2, trie(entry._1 + "~~"))
    }

    // Test prefixes of entries.
    testEntries.foreach { entry =>
      assertEquals(null, trie(entry._1.substring(0, 5)))
      assertEquals(null, trie(entry._1.substring(0, 5)))
    }

    // Test absurd values.
    assertEquals(null, trie(""))
    assertEquals(null, trie("~"))
  }

  @Test
  def testNormalAddingAndSquashedReading = {
    // Create a trie and add some test keys.
    val trie = new StringTrie[String]
    val testEntries = List("http://dbpedia.org/resource/" -> "dbpr",
      "http://dbpedia.org/resource/Category:" -> "dbpc",
      "http://xmlns.com/foaf/0.1/" -> "foaf",
      "urn:yahoo:maps" -> "y")
    testEntries.foreach(trie += _)

    // Squash the trie.
    trie.squash()

    // Test the entries directly.
    testEntries.foreach { entry =>
      assertEquals(entry._2, trie(entry._1))
    }

    // Test the entries with a suffix.
    testEntries.foreach { entry =>
      assertEquals(entry._2, trie(entry._1 + "~"))
      assertEquals(entry._2, trie(entry._1 + "~~"))
    }

    // Test prefixes of entries.
    testEntries.foreach { entry =>
      assertEquals(null, trie(entry._1.substring(0, 5)))
      assertEquals(null, trie(entry._1.substring(0, 5)))
    }

    // Test absurd values.
    assertEquals(null, trie(""))
    assertEquals(null, trie("~"))
  }

  @Test
  def testAssociatingEmptyValue = {
    val trie = new StringTrie[String]
    trie += "" -> "root"
    trie += "123" -> "not-root"

    0 to 1 foreach { i =>
      assertEquals("root", trie(""))
      assertEquals("root", trie("1"))
      assertEquals("root", trie("12"))
      assertEquals("not-root", trie("123"))
      assertEquals("not-root", trie("1234"))
      assertEquals("root", trie("abc"))
      assertEquals("" -> "root", trie.getKeyAndValue(""))
      assertEquals("" -> "root", trie.getKeyAndValue("1"))
      assertEquals("" -> "root", trie.getKeyAndValue("12"))
      assertEquals("123" -> "not-root", trie.getKeyAndValue("123"))
      assertEquals("123" -> "not-root", trie.getKeyAndValue("1234"))
      assertEquals("" -> "root", trie.getKeyAndValue("abc"))

      trie.squash()
    }


  }
}
