package de.hpi.isg.sodap.rdfind.plan

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators._
import de.hpi.isg.sodap.rdfind.operators.frequent_conditions.{CreateUnaryConditionEvidences, MergeUnaryConditionEvidences}
import de.hpi.isg.sodap.rdfind.plan.FrequentConditionPlanner.ConstructedDataSets
import de.hpi.isg.sodap.rdfind.programs.RDFind
import de.hpi.isg.sodap.rdfind.programs.RDFind._
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import de.hpi.isg.sodap.rdfind.util.{ConditionCodes, HashCollisionHandler, StringFunnel, UntypedBloomFilterParameters}
import org.apache.flink.api.java.io.PrintingOutputFormat
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.runtime.ScalaRunTime

/**
 * Plans [[DataSet]]s for frequent conditions, Bloom filters of them, and association rules of conditions.
 *
 * @author Sebastian
 * @since 10.04.2015.
 */
class FrequentConditionPlanner(parameters: RDFind.Parameters, baseBloomFilterParameters: UntypedBloomFilterParameters) {

  /**
   * Enhances the given dataset by a logic that finds frequent conditions within it.
   * @param triples is a data set of RDF triples
   * @param estTriples is the (estimated) number of triples within the RDF dataset
   * @return datasets containing various frequent-condition-related datasets
   */
  def constructFrequentConditionPlan(triples: DataSet[RDFTriple], estTriples: Long): ConstructedDataSets = {
    // Calculate an upper bound on the number of frequent distict values.
    val maxFrequentConditions = estTriples / parameters.minSupport
    LoggerFactory.getLogger(getClass).info(s"Expecting a maximum number of $maxFrequentConditions frequent conditions " +
      s"per condition type.")
    baseBloomFilterParameters.numExpectedElements = maxFrequentConditions.asInstanceOf[Int]

    var singleConditions: DataSet[SingleConditionCount] = null
    var doubleConditions: DataSet[DoubleConditionCount] = null
    if (parameters.frequentConditionStrategy == 0) {
      singleConditions = findFrequentSingleConditions(this.parameters.minSupport, triples)
    } else {
      val frequentConditions = findFrequentConditions(this.parameters.minSupport, triples)
      singleConditions = frequentConditions._1
      doubleConditions = frequentConditions._2
    }
    if (parameters.counterLevel >= 2) {
      singleConditions = singleConditions.filter(new CountItems[SingleConditionCount]("unary-conditions-counter"))
    }
    val singleConditionFilters = createFrequentSingleConditionBloomFilters(singleConditions)
      .filter(new CollectItems[(Int, BloomFilter[String])]("unary-conditions-bloomfilters"))

    var compressionDictionary: DataSet[(String, String)] = null
    var collisionHashes: DataSet[String] = null

    // If desired, compress the (allegedly) frequent values.
    if (this.parameters.isHashBasedDictionaryCompression) {
      // Create hashes for all (allegedly) single frequent conditions.
      val hashes = triples
        .flatMap(new CreateHashes(this.parameters.hashAlgorithm, this.parameters.hashBytes))
        .withBroadcastSet(singleConditionFilters, CreateHashes.UNARY_FC_BLOOM_FILTERS_BROADCAST)
        .name("Hash frequent unary condition values")

      // Combine values with the same hash.
      val hashGroups = hashes
        .groupBy(0)
        .reduce(new CombineHashes)
        .name("Combine equal-hash values")

      // Add collision-free hashes into the translation dictionary.
      compressionDictionary = hashGroups
        .filter(_._2.length == 1)
        .name("Filter hash-collision-free values")
        .map(hashGroup => (HashCollisionHandler.escapeHash(hashGroup._1), hashGroup._2(0)))
        .name("Create hash dictionary entries")

      // Collect colliding hashes.
      collisionHashes = hashGroups
        .filter(_._2.length > 1)
        .name("Filter hash-collision values")
        .map(_._1)
        .name("Project hash-collsion value")

      if (this.parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
        hashGroups
          .filter(_._2.length > 1)
          .map(x => s"Hash collision on ${x._1}: ${ScalaRunTime.stringOf(x._2)}")
          .output(new PrintingOutputFormat())
    }

    if (parameters.frequentConditionStrategy == 0) {
      doubleConditions = findFrequentDoubleConditions(triples, singleConditionFilters, this.parameters.minSupport, collisionHashes)
    }
    if (parameters.counterLevel >= 2) {
      doubleConditions = doubleConditions.filter(new CountItems[DoubleConditionCount]("binary-conditions-counter"))
    }
    val doubleConditionFilters = createFrequentDoubleConditionBloomFilters(doubleConditions)
      .filter(new CollectItems[(Int, BloomFilter[Condition])]("binary-conditions-bloomfilter"))

    var associationRules = findAssociationRules(singleConditions, doubleConditions, compressionDictionary, collisionHashes)
    if (parameters.counterLevel >= 1) {
      associationRules = associationRules.filter(new CountItems[AssociationRule]("association-rule-counter"))
    }

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE) {
      singleConditions.map(x => s"Frequent UC: $x").output(new PrintingOutputFormat())
      doubleConditions.map(x => s"Frequent BC: $x").output(new PrintingOutputFormat())
      associationRules.map(x => s"Association rule: $x").output(new PrintingOutputFormat())
    }

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS) {
      singleConditions.map(x => 1).reduce(_ + _).map(x => s"Found $x frequent single-conditions.").output(new PrintingOutputFormat())
      doubleConditions.map(x => 1).reduce(_ + _).map(x => s"Found $x frequent double-conditions.").output(new PrintingOutputFormat())
      associationRules.map(x => 1).reduce(_ + _).map(x => s"Found $x frequent association rules.").output(new PrintingOutputFormat())
    }

    ConstructedDataSets(singleConditions, singleConditionFilters,
      doubleConditions, doubleConditionFilters,
      associationRules, collisionHashes, compressionDictionary)
  }

  /**
   * Finds association rules between frequent single conditions.
   * @param frequentSingleItemSets are the frequent single conditions including support
   * @param frequentDoubleItemSets are the frequent double conditions including support
   * @return a [[DataSet]] containing the association rules
   */
  private def findAssociationRules(frequentSingleItemSets: DataSet[SingleConditionCount],
                                   frequentDoubleItemSets: DataSet[DoubleConditionCount],
                                   compressionDictionary: DataSet[(String, String)],
                                   collisionHashes: DataSet[String]):
  DataSet[AssociationRule] = {

    // Compress te frequent unary item sets.
    val unaryConditions =
      if (this.parameters.isHashBasedDictionaryCompression) {
        frequentSingleItemSets
          .map(new ConditionCompressor[SingleConditionCount](this.parameters.hashAlgorithm, this.parameters.hashBytes,
          _.value, _.value = _))
          .withBroadcastSet(collisionHashes, ConditionCompressor.COLLISION_HASHES)
          .name("Compress unary conditions")
      } else frequentSingleItemSets

    // Find the association rules based on the FIS.
    val associationRules1 = unaryConditions
      .filter(_.conditionType != objectCondition)
      .name("Filter non-object unary conditions")
      .join(frequentDoubleItemSets)
      .where("value").equalTo("value1") {
      (a: SingleConditionCount, b: DoubleConditionCount, out: Collector[AssociationRule]) =>
        if (a.conditionType == subjectCondition) {
          if (b.conditionType == subjectPredicateCondition) {
            out.collect(AssociationRule(subjectCondition, predicateCondition,
              b.value1, b.value2, b.count, b.count.asInstanceOf[Double] / a.count))
          } else if (b.conditionType == subjectObjectCondition) {
            out.collect(AssociationRule(subjectCondition, objectCondition,
              b.value1, b.value2, b.count, b.count.asInstanceOf[Double] / a.count))
          }
        } else if (a.conditionType == predicateCondition && b.conditionType == predicateObjectCondition) {
          out.collect(AssociationRule(predicateCondition, objectCondition,
            b.value1, b.value2, b.count, b.count.asInstanceOf[Double] / a.count))
        }
    }.name("Find s->p/s->o/p->s association rules")

    val associationRules2 = unaryConditions
      .filter(_.conditionType != subjectCondition)
      .name("Filter non-subject unary conditions")
      .join(frequentDoubleItemSets)
      .where("value").equalTo("value2") {
      (a: SingleConditionCount, b: DoubleConditionCount, out: Collector[AssociationRule]) =>
        if (a.conditionType == objectCondition) {
          if (b.conditionType == subjectObjectCondition) {
            out.collect(AssociationRule(objectCondition, subjectCondition,
              b.value2, b.value1, b.count, b.count.asInstanceOf[Double] / a.count))
          } else if (b.conditionType == predicateObjectCondition) {
            out.collect(AssociationRule(objectCondition, predicateCondition,
              b.value2, b.value1, b.count, b.count.asInstanceOf[Double] / a.count))
          }
        } else if (a.conditionType == predicateCondition && b.conditionType == subjectPredicateCondition) {
          out.collect(AssociationRule(predicateCondition, subjectCondition,
            b.value2, b.value1, b.count, b.count.asInstanceOf[Double] / a.count))
        }
    }.name("Find p->s/o->p/o->s association rules")

    val associationRules = associationRules1
      .union(associationRules2)
      .name("Union association rules")
      .filter(_.confidence == 1)
      .name("Filter perfect association rules")

    associationRules
  }

  /**
   * Creates Bloom filters for the frequent double item sets.
   * @param frequentDoubleItemSets contains the frequent double item sets
   * @return a data set with (type, Bloom filter) pairs
   */
  private def createFrequentDoubleConditionBloomFilters(frequentDoubleItemSets: DataSet[DoubleConditionCount]):
  DataSet[(Int, BloomFilter[Condition])] = {

    // Make sure that we do not need to serialize this whole class but only put this parameter in the scope.
    val bloomFilterParameters = baseBloomFilterParameters.withType[Condition](classOf[Condition.Funnel].getName)

    // Create Bloom filters for the double conditions for each partition of the dataset.
    val partialBloomFilters = frequentDoubleItemSets
      .mapPartition {
      conditions: Iterator[DoubleConditionCount] =>
        val bloomFilters = Map(subjectPredicateCondition -> bloomFilterParameters.createBloomFilter,
          subjectObjectCondition -> bloomFilterParameters.createBloomFilter,
          predicateObjectCondition -> bloomFilterParameters.createBloomFilter)

        while (conditions.hasNext) {
          val condition = conditions.next()
          bloomFilters(condition.conditionType).put(condition.scrapCount)
        }

        bloomFilters
    }.name("Create partial binary condition Bloom filters")

    // Merge the Bloom filters.
    partialBloomFilters
      .groupBy(0)
      .reduceGroup {
      (iterator: Iterator[(Int, BloomFilter[Condition])], out: Collector[(Int, BloomFilter[Condition])]) =>
        val bloomFilterEntry = iterator.next()
        while (iterator.hasNext) {
          val nextBloomFilterEntry = iterator.next()
          bloomFilterEntry._2.putAll(nextBloomFilterEntry._2)
        }
        out.collect(bloomFilterEntry)
    }.name("Merge partial binary condition Bloom filters")
  }

  /**
   * Creates Bloom filters for frequent 1-conditions.
   *
   * @param frequentSingleItemSets are the frequent conditions
   * @return a [[DataSet]] containing Bloom filters for frequent conditions by condition type
   */
  private def createFrequentSingleConditionBloomFilters(frequentSingleItemSets: DataSet[SingleConditionCount]):
  DataSet[(Int, BloomFilter[String])] = {

    // Make sure that we do not need to serialize this whole class but only put this parameter in the scope.
    val bloomFilterParameters = baseBloomFilterParameters.withType[String](classOf[StringFunnel].getName)

    // Create Bloom filters for the single FIS.
    val partialBloomFilters = frequentSingleItemSets
      .mapPartition {
      conditions: Iterator[SingleConditionCount] =>
        // TODO: Think about how to optimize these parameters...
        val bloomFilters = Map(subjectCondition -> bloomFilterParameters.createBloomFilter,
          predicateCondition -> bloomFilterParameters.createBloomFilter,
          objectCondition -> bloomFilterParameters.createBloomFilter)

        while (conditions.hasNext) {
          val condition = conditions.next()
          bloomFilters(condition.conditionType).put(condition.value)
        }
        bloomFilters
    }.name("Create partial unary condition Bloom filters")

    val singleFisBloomFilters = partialBloomFilters
      .groupBy(0)
      .reduceGroup { (iterator: Iterator[(Int, BloomFilter[String])], out: Collector[(Int, BloomFilter[String])]) =>
      var bloomFilterType = 0
      var bloomFilter: BloomFilter[String] = null
      while (iterator.hasNext) {
        val input = iterator.next()
        bloomFilterType = input._1
        if (bloomFilter == null) {
          bloomFilter = input._2
        } else {
          bloomFilter.putAll(input._2)
        }
      }
      out.collect((bloomFilterType, bloomFilter))
    }.name("Merge partial unary condition Bloom filters")

    singleFisBloomFilters
  }

  /**
   * Finds frequent item sets.
   * @param minSupport is the lower threshold of items to be considered frequent
   * @param triples contains the potentially frequent items
   * @return a [[DataSet]] with frequent items/conditions and their support
   */
  private def findFrequentSingleConditions(minSupport: Int, triples: DataSet[RDFTriple]): DataSet[SingleConditionCount] = {
    // Find the single FIS.
    val frequentSingleItemSets = triples
      .flatMap { (triple: RDFTriple, out: Collector[SingleConditionCount]) =>
      val output = SingleConditionCount(subjectCondition, triple.subj, 1)
      out.collect(output)

      output.conditionType = predicateCondition
      output.value = triple.pred
      out.collect(output)

      output.conditionType = objectCondition
      output.value = triple.obj
      out.collect(output)
    }.name("Create unary condition counts")
      .groupBy("conditionType", "value")
      .sum("count")
      .filter(_.count >= minSupport)
      .name("Filter frequent unary condition counts")
    frequentSingleItemSets
  }

  /**
   * Finds frequent conditions in a single pass over the data.
   * @param minSupport is the lower threshold of items to be considered frequent
   * @param triples contains the potentially frequent items
   * @return the unary and binary condition datasets
   */
  private def findFrequentConditions(minSupport: Int, triples: DataSet[RDFTriple]):
  (DataSet[SingleConditionCount], DataSet[DoubleConditionCount]) = {
    // Find the frequent unary conditions.
    val frequentUnaryConditionEvidences = triples
      .flatMap(new CreateUnaryConditionEvidences).name("Create unary condition counts")
      .groupBy("conditionType", "value")
      .reduceGroup(new MergeUnaryConditionEvidences)
      .name("Merge unary condition evidences")
      .filter(_.count >= minSupport)
      .name("Filter frequent unary condition counts")

    val frequentUnaryConditions = frequentUnaryConditionEvidences
      .map { evidence => SingleConditionCount(evidence.conditionType, evidence.value, evidence.count) }
      .name("Strip triple IDs")

    val doubleConditionCounts = frequentUnaryConditionEvidences
      .flatMap { (unaryConditionEvidence: UnaryConditionEvidence, out: Collector[(Long, Int, String)]) =>
      for (id <- unaryConditionEvidence.tripleIds) {
        out.collect((id, unaryConditionEvidence.conditionType, unaryConditionEvidence.value))
      }
    }
      .name("Split frequent unary condition evidences")
      .groupBy(0)
      .reduceGroup { (in: Iterator[(Long, Int, String)], out: Collector[DoubleConditionCount]) =>
      var elements = in.toSeq
      if (elements.size > 1) {
        elements = elements.sortBy(_._2)
        val element1 = elements(0)
        val element2 = elements(1)
        out.collect(DoubleConditionCount(ConditionCodes.merge(element1._2, element2._2), element1._3, element2._3, 1))
        if (elements.size > 2) {
          val element3 = elements(2)
          out.collect(DoubleConditionCount(ConditionCodes.merge(element1._2, element3._2), element1._3, element3._3, 1))
          out.collect(DoubleConditionCount(ConditionCodes.merge(element2._2, element3._2), element2._3, element3._3, 1))
        }
      }
    }
      .name("Create binary condition counts")

    val frequentBinaryConditions = doubleConditionCounts
      .groupBy("conditionType", "value1", "value2")
      .sum("count")
      .filter(_.count >= minSupport)
      .name("Filter frequent binary conditions")

    (frequentUnaryConditions, frequentBinaryConditions)
  }

  /**
   * Finds frequent item sets.
   * @param triples in which the frequent item sets shall be found
   * @param singleFisBloomFilters heuristically contains the frequent items
   * @param minSupport is the lower support for item sets to be considered frequent
   * @return a [[DataSet]] containing the frequent conditions and their support
   */
  private def findFrequentDoubleConditions(triples: DataSet[RDFTriple],
                                           singleFisBloomFilters: DataSet[(Int, BloomFilter[String])],
                                           minSupport: Int,
                                           collisionHashes: DataSet[String]):
  DataSet[DoubleConditionCount] = {

    val isUseDictionaryCompression = collisionHashes != null
    val doubleConditionCounts = triples
      .flatMap(new CreatedReducedDoubleConditionCounts(isUseDictionaryCompression,
      this.parameters.hashAlgorithm, this.parameters.hashBytes))
      .withBroadcastSet(singleFisBloomFilters, singleFisBroadcast)
      .name("Create binary condition counts")
    if (isUseDictionaryCompression) {
      doubleConditionCounts.withBroadcastSet(collisionHashes, CreatedReducedDoubleConditionCounts.COLLISION_HASHES)
    }
    doubleConditionCounts
      .groupBy("conditionType", "value1", "value2")
      .sum("count")
      .filter(_.count >= minSupport)
      .name("Filter frequent binary conditions")
  }


}

object FrequentConditionPlanner {

  // TODO
  case class ConstructedDataSets(frequentSingleConditions: DataSet[SingleConditionCount],
                                 frequentSingleConditionBloomFilter: DataSet[(Int, BloomFilter[String])],
                                 frequentDoubleConditions: DataSet[DoubleConditionCount],
                                 frequentDoubleConditionBloomFilter: DataSet[(Int, BloomFilter[Condition])],
                                 associationRules: DataSet[AssociationRule],
                                 collisionHashes: DataSet[String],
                                 dictionary: DataSet[(String, String)]) {
  }

  lazy val noDataSets = ConstructedDataSets(null, null, null, null, null, null, null)

}
