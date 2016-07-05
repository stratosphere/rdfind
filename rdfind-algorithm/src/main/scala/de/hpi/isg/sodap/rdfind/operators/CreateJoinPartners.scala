package de.hpi.isg.sodap.rdfind.operators

import java.lang.Iterable

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data.{AssociationRule, Condition, JoinCandidate, RDFTriple}
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import de.hpi.isg.sodap.rdfind.util.{HashCollisionHandler, HashFunction}
import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichFlatMapFunction}
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
class CreateJoinPartners(isUseUnaryFcBloomFilter: Boolean,
                         isUseBinaryFcFilter: Boolean,
                         isUseAssociationRules: Boolean,
                         projectionAttributes: String,
                         isUseDictionaryCompression: Boolean,
                         hashAlgorithm: String = HashFunction.DEFAULT_ALGORITHM,
                         hashBytes: Int = -1)
  extends RichFlatMapFunction[RDFTriple, JoinCandidate] {


  var unaryFcBloomFilter: Map[Int, BloomFilter[String]] = _
  var binaryFcBloomFilter: Map[Int, BloomFilter[Condition]] = _

  val isProjectSubjects = projectionAttributes.contains("s")
  val isProjectPredicates = projectionAttributes.contains("p")
  val isProjectObjects = projectionAttributes.contains("o")

  lazy val hashFunction = new HashFunction(hashAlgorithm, hashBytes)
  var hashCollisionHandler: HashCollisionHandler = _

  lazy val doubleConditionDummy = Condition(null, null, 0)

  lazy val testCondition = Condition(null, null, 0)
  var associationRuleImpliedConditions: Set[Condition] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    if (isUseUnaryFcBloomFilter) {
      val broadcastVariable = getRuntimeContext.getBroadcastVariable[(Int, BloomFilter[String])](
        CreateJoinPartners.UNARY_FC_BLOOM_FILTERS_BROADCAST)
      unaryFcBloomFilter = broadcastVariable.toMap
    }

    if (isUseBinaryFcFilter) {
      val broadcastVariable = getRuntimeContext.getBroadcastVariable[(Int, BloomFilter[Condition])](
        CreateJoinPartners.BINARY_FC_BLOOM_FILTERS_BROADCAST)
      binaryFcBloomFilter = broadcastVariable.toMap
    }

    if (this.isUseDictionaryCompression) {
      val collisionHashes = getRuntimeContext
        .getBroadcastVariable[String](CreateJoinPartners.COLLISION_HASHES_BROADCAST)
        .toSet
      this.hashCollisionHandler = new HashCollisionHandler(this.hashFunction, collisionHashes)
    }

    if (this.isUseAssociationRules) {
      this.associationRuleImpliedConditions = getRuntimeContext
        .getBroadcastVariableWithInitializer[AssociationRule, Set[Condition]](
          CreateJoinPartners.ASSOCIATION_RULE_BROADCAST,
          CreateJoinPartners.AssocationRuleBroadcastInitializer)
    }
  }

  private def mightBeFrequent(value1: String, value2: String, conditionType: Int): Boolean = !this.isUseBinaryFcFilter || {
    val bloomFilter = this.binaryFcBloomFilter(conditionType)
    this.doubleConditionDummy.conditionValue1 = value1;
    this.doubleConditionDummy.conditionValue2 = value2;
    this.doubleConditionDummy.conditionType = conditionType;
    bloomFilter.mightContain(this.doubleConditionDummy)
  }

  override def flatMap(triple: RDFTriple, out: Collector[JoinCandidate]): Unit = {
    var s = if (!isUseUnaryFcBloomFilter || unaryFcBloomFilter(subjectCondition).mightContain(triple.subj)) triple.subj else null
    var p = if (!isUseUnaryFcBloomFilter || unaryFcBloomFilter(predicateCondition).mightContain(triple.pred)) triple.pred else null
    var o = if (!isUseUnaryFcBloomFilter || unaryFcBloomFilter(objectCondition).mightContain(triple.obj)) triple.obj else null

    if (this.isUseDictionaryCompression) {
      s = applyDictionaryCompression(s)
      p = applyDictionaryCompression(p)
      o = applyDictionaryCompression(o)
    }

    // Create join candidates on the object value.
    if (this.isProjectObjects) {
      if (s != null) {
        if (p != null && mightBeFrequent(s, p, subjectPredicateCondition) && !isAssociationRuleImplied(s, p, subjectPredicateCondition)) {
          out.collect(JoinCandidate(triple.obj, s, p, addSecondaryConditions(subjectPredicateCondition)))
        } else if (p != null) {
          out.collect(JoinCandidate(triple.obj, p, CreateJoinPartners.NO_VALUE,
              createConditionCode(predicateCondition, secondaryCondition = objectCondition)))
        }
        out.collect(JoinCandidate(triple.obj, s, CreateJoinPartners.NO_VALUE,
          createConditionCode(subjectCondition, secondaryCondition = objectCondition)))
      } else if (p != null) {
        out.collect(JoinCandidate(triple.obj, p, CreateJoinPartners.NO_VALUE,
          createConditionCode(predicateCondition, secondaryCondition = objectCondition)))
      }
    }

    // Create join candidates on the predicate value.
    if (this.isProjectPredicates) {
      if (s != null) {
        if (o != null && mightBeFrequent(s, o, subjectObjectCondition) && !isAssociationRuleImplied(s, o, subjectObjectCondition)) {
          out.collect(JoinCandidate(triple.pred, s, o, addSecondaryConditions(subjectObjectCondition)))
        } else if (o != null) {
          out.collect(JoinCandidate(triple.pred, o, CreateJoinPartners.NO_VALUE,
              createConditionCode(objectCondition, secondaryCondition = predicateCondition)))
        }
        out.collect(JoinCandidate(triple.pred, s, CreateJoinPartners.NO_VALUE,
          createConditionCode(subjectCondition, secondaryCondition = predicateCondition)))
      } else if (o != null) {
        out.collect(JoinCandidate(triple.pred, o, CreateJoinPartners.NO_VALUE,
          createConditionCode(objectCondition, secondaryCondition = predicateCondition)))
      }
    }

    // Create join candidates on the subject value.
    if (this.isProjectSubjects) {
      if (p != null) {
        if (o != null && mightBeFrequent(p, o, predicateObjectCondition) && !isAssociationRuleImplied(p, o, predicateObjectCondition)) {
          out.collect(JoinCandidate(triple.subj, p, o, addSecondaryConditions(predicateObjectCondition)))
        } else if (o != null) {
          out.collect(JoinCandidate(triple.subj, o, CreateJoinPartners.NO_VALUE,
              createConditionCode(objectCondition, secondaryCondition = subjectCondition)))
        }
        out.collect(JoinCandidate(triple.subj, p, CreateJoinPartners.NO_VALUE,
          createConditionCode(predicateCondition, secondaryCondition = subjectCondition)))
      } else if (o != null) {
        out.collect(JoinCandidate(triple.subj, o, CreateJoinPartners.NO_VALUE,
          createConditionCode(objectCondition, secondaryCondition = subjectCondition)))
      }
    }
  }

  def applyDictionaryCompression(originalValue: String): String =
    if (originalValue != null) this.hashCollisionHandler.hashAndResolveCollision(originalValue)
    else null

  def replaceNullsWithEmptyString(str: String): String = if (str == null) "" else str

  /**
   * Tests whether the given condition is equal to another condition because of a known association rule.
   */
  @inline
  def isAssociationRuleImplied(value1: String, value2: String, conditionType: Int) = {
    this.isUseAssociationRules && {
      this.testCondition.conditionValue1 = value1
      this.testCondition.conditionValue2 = value2
      this.testCondition.conditionType = conditionType

      this.associationRuleImpliedConditions(this.testCondition)
    }
  }
}

object CreateJoinPartners {
  val UNARY_FC_BLOOM_FILTERS_BROADCAST = "unary-fc-bloom-filters"
  val BINARY_FC_BLOOM_FILTERS_BROADCAST = "binary-fc-bloom-filters"
  val COLLISION_HASHES_BROADCAST = "collision-hashes"
  val ASSOCIATION_RULE_BROADCAST = "association-rules"
  val NO_VALUE: String = null

  // or ""

  /**
   * Merges the antecedent and descendent of association rules into a condition. This condition produces a capture
   * that is equal to the capture from only the antecedent.
   */
  object AssocationRuleBroadcastInitializer extends BroadcastVariableInitializer[AssociationRule, Set[Condition]] {

    override def initializeBroadcastVariable(associationRules: Iterable[AssociationRule]): Set[Condition] = {
      associationRules
        .map { associationRule =>
        val conditionType = associationRule.antecedentType | associationRule.consequentType
        if (associationRule.antecedentType < associationRule.consequentType)
          Condition(associationRule.antecedent, associationRule.consequent, conditionType)
        else
          Condition(associationRule.consequent, associationRule.antecedent, conditionType)
      }.toSet
    }

  }

}