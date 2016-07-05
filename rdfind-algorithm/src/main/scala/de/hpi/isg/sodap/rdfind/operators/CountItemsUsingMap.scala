package de.hpi.isg.sodap.rdfind.operators

import org.apache.flink.api.common.functions.{RichMapFunction, RichFilterFunction}
import org.apache.flink.configuration.Configuration

/**
 * @author sebastian.kruse 
 * @since 22.06.2015
 */
class CountItemsUsingMap[T](accumulatorName: String) extends RichMapFunction[T, T] {

  private var counter: Long = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    this.counter = 0
  }


  override def map(t: T): T = {
    this.counter += 1
    t
  }

  override def close(): Unit = {
    val accumulator = getRuntimeContext.getLongCounter(accumulatorName)
    accumulator.add(this.counter)

    super.close()
  }
}
