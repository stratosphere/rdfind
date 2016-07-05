package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.util.{HashCollisionHandler, HashFunction}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
 * Created by basti on 5/19/15.
 */
class ConditionCompressor[T](hashAlgorithm: String = HashFunction.DEFAULT_ALGORITHM,
                             hashBytes: Int = -1,
                             extractionFunc: T => String,
                             replaceFunc: (T, String) => Unit)
  extends RichMapFunction[T, T] {

  lazy val hashFunction = new HashFunction(hashAlgorithm, hashBytes)
  var hashCollisionHandler: HashCollisionHandler = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val collisionHashes = getRuntimeContext
      .getBroadcastVariable[String](ConditionCompressor.COLLISION_HASHES)
      .toSet
    this.hashCollisionHandler = new HashCollisionHandler(this.hashFunction, collisionHashes)
  }

  override def map(condition: T): T = {
    replaceFunc(condition, this.hashCollisionHandler.hashAndResolveCollision(extractionFunc(condition)))
    condition
  }
}

object ConditionCompressor {
  val COLLISION_HASHES = "collision-hashes"
}
