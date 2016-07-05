package de.hpi.isg.sodap.rdfind.operators

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * @author sebastian.kruse 
 * @since 22.06.2015
 */
class CountItems[T](accumulatorName: String) extends RichFilterFunction[T] {

  private var counter: Long = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    this.counter = 0
  }


  override def filter(t: T): Boolean = {
    this.counter += 1
    true
  }

  override def close(): Unit = {
    val accumulator = getRuntimeContext.getLongCounter(accumulatorName)
    accumulator.add(this.counter)

    super.close()
  }
}
