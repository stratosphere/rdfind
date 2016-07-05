package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data.RDFTriple
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author sebastian.kruse 
 * @since 02.07.2015
 */
class AsciifyTriples extends MapFunction[RDFTriple, RDFTriple] {

  private lazy val sb = new StringBuilder

  /**
   * Turns a unicode string into a string that only contains ASCII symbols.
   */
  private def asciify(str: String): String = {

    0 until str.length foreach { pos =>
      var char = str.charAt(pos)
      // Test if we encounter a non-ASCII character.
      if (char > 0x7F || this.sb.nonEmpty) {
        // Lazy-fill the buffer.
        if (this.sb.isEmpty) (0 until pos).foreach((pos: Int) => sb.append(str.charAt(pos)))
        // Add ASCIIfied char.
        do {
          this.sb.append((char & 0x7F).asInstanceOf[Char])
          char = (char >>> 7).asInstanceOf[Char]
        } while (char != 0)
      }
    }

    val returnValue = if (this.sb.isEmpty) str else sb.toString
    sb.clear()

    returnValue
  }

  override def map(t: RDFTriple): RDFTriple = {
    t.subj = asciify(t.subj)
    t.pred = asciify(t.pred)
    t.obj = asciify(t.obj)
    t
  }

}
