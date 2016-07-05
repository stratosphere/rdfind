package de.hpi.isg.sodap.rdfind.operators

import org.apache.flink.api.common.accumulators.ListAccumulator
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * @author sebastian.kruse
  * @since 22.06.2015
  */
class CollectItems[T](accumulatorName: String) extends RichFilterFunction[T] {

   private var accumulator: ListAccumulator[String] = _


   override def open(parameters: Configuration): Unit = {
     super.open(parameters)
     this.accumulator = new ListAccumulator[String]
     getRuntimeContext.addAccumulator(accumulatorName, this.accumulator)
   }

   override def filter(t: T): Boolean = {
     this.accumulator.add(scala.runtime.ScalaRunTime.stringOf(t))
     true
   }
 }
