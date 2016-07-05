package de.hpi.isg.sodap.rdfind.operators

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data.RDFTriple
import de.hpi.isg.sodap.rdfind.operators.CreateHashes._
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import de.hpi.isg.sodap.rdfind.util.HashFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._


/**
 * Creates all possible join partners for an RDF triple. Each join partner consists of a join value
 * and the condition that is associated with this value. If the condition values are not frequent, no
 * join partner is created. If only one condition value is frequent, the other value will be nullified.
 * @author sebastian.kruse
 * @since 08.04.2015
 */
class CreateHashes(hashAlgorithm: String, hashBytes: Int = -1)
  extends RichFlatMapFunction[RDFTriple, (String, Array[String])] {

  var unaryFcBloomFilter: Map[Int, BloomFilter[String]] = _

  lazy val hashFunction = new HashFunction(hashAlgorithm, hashBytes, isUnsetFirstMsb = false)

  lazy val singletonArray = Array("")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

      val broadcastVariable = getRuntimeContext.getBroadcastVariable[(Int, BloomFilter[String])](
        UNARY_FC_BLOOM_FILTERS_BROADCAST)
      unaryFcBloomFilter = broadcastVariable.toMap
  }


  override def flatMap(triple: RDFTriple, out: Collector[(String, Array[String])]): Unit = {
    val s = if (unaryFcBloomFilter(subjectCondition).mightContain(triple.subj)) triple.subj else null
    val p = if (unaryFcBloomFilter(predicateCondition).mightContain(triple.pred)) triple.pred else null
    val o = if (unaryFcBloomFilter(objectCondition).mightContain(triple.obj)) triple.obj else null

    if (s != null) {
      this.singletonArray(0) = s
      out.collect((this.hashFunction.hashStringToString(s), this.singletonArray))
    }
    if (p != null) {
      this.singletonArray(0) = p
      out.collect((this.hashFunction.hashStringToString(p), this.singletonArray))
    }
    if (o != null) {
      this.singletonArray(0) = o
      out.collect((this.hashFunction.hashStringToString(o), this.singletonArray))
    }
  }

  def replaceNullsWithEmptyString(str: String): String = if (str == null) "" else str
}

object CreateHashes {
  val UNARY_FC_BLOOM_FILTERS_BROADCAST = CreateJoinPartners.UNARY_FC_BLOOM_FILTERS_BROADCAST
}

