package de.hpi.isg.sodap.rdfind.plan

import java.util

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators._
import de.hpi.isg.sodap.rdfind.operators.candidate_extraction.CreateAllCindCandidates
import de.hpi.isg.sodap.rdfind.operators.candidate_merging.IntersectCindCandidates
import de.hpi.isg.sodap.rdfind.plan.FrequentConditionPlanner.ConstructedDataSets
import de.hpi.isg.sodap.rdfind.programs.RDFind
import de.hpi.isg.sodap.rdfind.programs.RDFind.Parameters
import de.hpi.isg.sodap.rdfind.util.ConditionCodes
import de.hpi.isg.sodap.util.gp.CollectionUtils
import org.apache.flink.api.java.io.PrintingOutputFormat
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector

/**
 * This traversal strategy tests all possible CINDs of an RDF dataset at once.
 *
 * @author Sebastian Kruse
 * @since 14.04.2015.
 */
class AllAtOnceTraversalStrategy extends TraversalStrategy {
  /**
   * Builds a Flink subplan, that extracts CINDs from joined RDF triples.
   * @param tripleJoin contains all co-occurring, frequent conditions from the RDF data set
   * @param frequentConditionDataSets contains frequent conditions, Bloom filters, and association rules
   * @param parameters is the configuration of the RDFind job
   * @return TODO
   */
  override def enhanceFlinkPlan(tripleJoin: DataSet[JoinLine],
                                frequentConditionDataSets: ConstructedDataSets,
                                frequentCapturesBloomFilter: DataSet[BloomFilter[Condition]],
                                parameters: Parameters): DataSet[Cind] = {

    val minSupport = parameters.minSupport
    val isReevaluateBinaryCaptureFrequency = parameters.isUseFrequentItemSet && parameters.isCreateAnyBinaryCaptures

    // Create CIND candidates from each capture group.
    val indCandidates = tripleJoin
      .flatMap(new CreateAllCindCandidates(isReevaluateBinaryCaptureFrequency, parameters.isUseAssociationRules,
      splitStrategy = parameters.rebalanceSplitStrategy,
      isUsingFrequentCapturesBloomFilter = parameters.isFindFrequentCaptures))
      .name("Create all CIND evidences")
    if (isReevaluateBinaryCaptureFrequency) {
      indCandidates.withBroadcastSet(frequentConditionDataSets.frequentDoubleConditionBloomFilter,
        CreateAllCindCandidates.FREQUENT_BINARY_CONDITIONS_BROADCAST)
    }
    if (parameters.isUseAssociationRules) {
      indCandidates.withBroadcastSet(frequentConditionDataSets.associationRules,
        CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST)
    }
    if (parameters.isFindFrequentCaptures) {
      indCandidates.withBroadcastSet(frequentCapturesBloomFilter, CreateDependencyCandidates.FREQUENT_CAPTURES_BROADCAST)
    }

    // Merge the CIND candidates.
    val cindSets =
      (if (!parameters.isNotBulkMerge) {
        indCandidates
          .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
          .combineGroup(new IntersectCindCandidates())
          .name("Intersect CIND evidences")
          .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
          .reduceGroup(new IntersectCindCandidates())
          .name("Intersect CIND evidences")
      } else {
        indCandidates
          .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
          .reduce {
          (candidate1: CindSet, candidate2: CindSet) =>
            val intersectedConditions: util.List[Condition] = new util.LinkedList[Condition]
            CollectionUtils.intersectAll(intersectedConditions, candidate1.refConditions, candidate2.refConditions)
            candidate1.refConditions = intersectedConditions.toArray(new Array[Condition](intersectedConditions.size))
            candidate1.depCount += candidate2.depCount
            candidate1
        }.name("Intersect CIND evidences")
      }).filter {
        _.depCount >= minSupport
      }.name("Filter infrequent CIND sets")

    splitAndCleanCindSets(parameters, cindSets)
  }

  /**
   * @return whether this strategy can incorporate frequent conditions.
   */
  override def isAbleToUseFrequentConditions: Boolean = true

  override def extractJoinLineSize: (JoinLine) => Int = _.numCombinedConditions

  override def extractMaximumJoinLineSize: (JoinLineLoad) => Int = _.combinedBlockSize
}
