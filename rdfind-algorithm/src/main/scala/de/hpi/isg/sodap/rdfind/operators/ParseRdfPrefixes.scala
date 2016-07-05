package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data.{RDFTriple, RDFPrefix}
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.util.Collector

/**
 * @author sebastian.kruse 
 * @since 18.06.2015
 */
class ParseRdfPrefixes extends MapFunction[Tuple2[Integer, String], RDFPrefix] {

  lazy val prefixPattern = """@prefix\s+(\S+): <(\S+)>\s*\.\n?""".r
  lazy val basePattern = """@prefix\s+<(\S+)>\s*\.\n?""".r

  override def map(value: Tuple2[Integer, String]): RDFPrefix = {
    value.f1 match {
      case prefixPattern(prefix, url) => RDFPrefix(prefix, url)
      case basePattern(url) => RDFPrefix("", url)
      case _ => {
        val msg = s"Could not parse the line $value correctly."
        throw new IllegalArgumentException(msg)
      }
    }
  }

}
