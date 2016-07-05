package de.hpi.isg.sodap.rdfind.util

import com.google.common.hash.{PrimitiveSink, Funnel}

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
class StringFunnel extends Funnel[String] {

  override def funnel(t: String, primitiveSink: PrimitiveSink): Unit = primitiveSink.putUnencodedChars(t)

  override def equals(o: scala.Any): Boolean = o != null && o.isInstanceOf[StringFunnel]
}
