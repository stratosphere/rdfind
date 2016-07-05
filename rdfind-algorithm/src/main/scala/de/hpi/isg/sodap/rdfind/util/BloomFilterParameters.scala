package de.hpi.isg.sodap.rdfind.util

import com.google.common.hash.data.BitArray
import com.google.common.hash.{SpectralBloomFilter, Funnel, BloomFilter}

/**
 * @author sebastian.kruse 
 * @since 10.06.2015
 */
case class BloomFilterParameters[T](var numExpectedElements: Int,
                                    var desiredFpp: Double,
                                    var funnelClass: String,
                                    var numBitsPerPosition: Int = 8) extends Serializable {

  if (numExpectedElements == 0) {
    numExpectedElements = 1
  }

  def createBloomFilter = BloomFilter.create[T](createFunnel(funnelClass), numExpectedElements, desiredFpp)

  /**
   * Creates a new Bloom filter that wraps the given bits. The number of hash functions in the Bloom filter is
   * exclusively based on the parameters in this object, but not on the number of given bits.
   * @param bits are the bits to use in the new Bloom filter
   * @return a new Bloom filter instance
   */
  def createBloomFilter(bits: BitArray) = {
    val optimalNumberOfBits = BloomFilter.optimalNumOfBits(numExpectedElements, desiredFpp)
    val numHashFunctions = BloomFilter.optimalNumOfHashFunctions(numExpectedElements, optimalNumberOfBits)
    new BloomFilter[T](bits, numHashFunctions, createFunnel(funnelClass), BloomFilter.DEFAULT_STRATEGY)
  }

  def createSpectralBloomFilter =
    SpectralBloomFilter.create[T](numBitsPerPosition, createFunnel(funnelClass), numExpectedElements, desiredFpp)

  def createFunnel(funnelClass: String): Funnel[T] = Class.forName(funnelClass).newInstance().asInstanceOf[Funnel[T]]

}
