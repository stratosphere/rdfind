package de.hpi.isg.sodap.rdfind.operators

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data.{DoubleConditionCount, RDFTriple}
import de.hpi.isg.sodap.rdfind.programs.RDFind
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import de.hpi.isg.sodap.rdfind.util.{HashCollisionHandler, HashFunction}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
 * This function extracts potential frequent double conditions from a set of RDF triples based on frequent single
 * conditions that are provided by a Bloom filter.
 *
 * @author sebastian.kruse 
 * @since 08.04.2015
 */
class CreatedReducedDoubleConditionCounts(isUseDictionaryCompression: Boolean = false,
                                          hashAlgorithm: String = HashFunction.DEFAULT_ALGORITHM,
                                          hashBytes: Int = -1)
  extends RichFlatMapFunction[RDFTriple, DoubleConditionCount] {

  var bloomFilters: Map[Int, BloomFilter[String]] = _

  lazy val hashFunction = new HashFunction(hashAlgorithm, hashBytes)
  var hashCollisionHandler: HashCollisionHandler = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val broadcastVariable = getRuntimeContext.getBroadcastVariable[(Int, BloomFilter[String])](RDFind.singleFisBroadcast)
    bloomFilters = broadcastVariable.toMap

    if (this.isUseDictionaryCompression) {
      val collisionHashes = getRuntimeContext
        .getBroadcastVariable[String](CreatedReducedDoubleConditionCounts.COLLISION_HASHES)
        .toSet
      this.hashCollisionHandler = new HashCollisionHandler(this.hashFunction, collisionHashes)
    }
  }

  override def flatMap(triple: RDFTriple, out: Collector[DoubleConditionCount]): Unit = {
    var matchCount = 0
    var s = if (bloomFilters(subjectCondition).mightContain(triple.subj)) {
      matchCount += 1;
      triple.subj
    } else null
    var p = if (bloomFilters(predicateCondition).mightContain(triple.pred)) {
      matchCount += 1;
      triple.pred
    } else null
    var o = if (bloomFilters(objectCondition).mightContain(triple.obj)) {
      matchCount += 1;
      triple.obj
    } else null

    if (matchCount < 2) {
      return
    }

    if (this.isUseDictionaryCompression) {
      s = applyDictionaryCompression(s)
      p = applyDictionaryCompression(p)
      o = applyDictionaryCompression(o)
    }

    var numOutputs = 0
    if (s != null) {
      if (p != null) {
        out.collect(DoubleConditionCount(subjectPredicateCondition, s, p, 1))
        numOutputs += 1
      }
      if (o != null) {
        out.collect(DoubleConditionCount(subjectObjectCondition, s, o, 1))
        numOutputs += 1
      }
    }

    if (p != null && o != null) {
      out.collect(DoubleConditionCount(predicateObjectCondition, p, o, 1))
      numOutputs += 1
    }
  }

  def applyDictionaryCompression(originalValue: String): String =
    if (originalValue != null) this.hashCollisionHandler.hashAndResolveCollision(originalValue)
    else null

}

object CreatedReducedDoubleConditionCounts {
  val COLLISION_HASHES = "collision-hashes"
}