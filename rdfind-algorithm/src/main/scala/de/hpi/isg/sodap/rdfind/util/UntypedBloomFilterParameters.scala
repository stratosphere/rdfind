package de.hpi.isg.sodap.rdfind.util

import com.google.common.hash.Funnel

/**
 * @author sebastian.kruse 
 * @since 10.06.2015
 */
case class UntypedBloomFilterParameters(var numExpectedElements: Int, var desiredFpp: Double) {

  def withType[T](funnelClass: String) = BloomFilterParameters[T](numExpectedElements, desiredFpp, funnelClass)

  def withType[T](funnelClass: Class[Funnel[T]]): BloomFilterParameters[T] = withType[T](funnelClass.getName)

}
