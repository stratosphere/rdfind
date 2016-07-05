package de.hpi.isg.sodap.rdfind.util

/**
 * @author sebastian.kruse 
 * @since 05.05.2015
 */
trait NullSensitiveOrdered[T] extends Ordered[T] {

  def compareNullSensitive[X](first: X with Comparable[X], second: X): Int = {
    return if (first == null) {
      if (second == null) 0 else -1
    } else {
      if (second == null) 1 else first.compareTo(second)
    }
  }

}
