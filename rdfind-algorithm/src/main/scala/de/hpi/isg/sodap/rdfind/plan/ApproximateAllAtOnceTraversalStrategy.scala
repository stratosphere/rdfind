package de.hpi.isg.sodap.rdfind.plan

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators._
import de.hpi.isg.sodap.rdfind.operators.candidate_merging.{IntersectCindCandidates, IntersectHalfApproximateCindCandidates}
import de.hpi.isg.sodap.rdfind.plan.FrequentConditionPlanner.ConstructedDataSets
import de.hpi.isg.sodap.rdfind.programs.RDFind.Parameters
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector

/**
 * This traversal strategy tests all possible CINDs of an RDF dataset at once.
 *
 * @author Sebastian Kruse
 * @since 14.04.2015.
 */
class ApproximateAllAtOnceTraversalStrategy extends TraversalStrategy {
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
    val exactnessThreshold = parameters.explicitCandidateThreshold
    val parallelism = parameters.stratosphereParameters.degreeOfParallelism

    val bloomFilterParameters = BloomFilterParameters[Condition](exactnessThreshold * 10, 0.1, new Condition.Funnel().getClass.getName)

    val createAllHalfApproximateCindCandidates = new CreateAllHalfApproximateCindCandidates(
      exactnessThreshold, bloomFilterParameters, isUseAssociationRules = parameters.isUseAssociationRules,
    splitStrategy = parameters.rebalanceSplitStrategy,
    isUseFrequentCapturesBloomFilter = parameters.isFindFrequentCaptures)


    // Create CINDs as Bloom filters.
    val approximateCindCandidates = tripleJoin
      .flatMap(createAllHalfApproximateCindCandidates)
      .name("Create half approximated CIND evidences")
    if (isReevaluateBinaryCaptureFrequency) {
      approximateCindCandidates.withBroadcastSet(frequentConditionDataSets.frequentDoubleConditionBloomFilter,
        CreateAllCindCandidatesApproximate.FREQUENT_BINARY_CONDITIONS_BROADCAST)
    }
    if (parameters.isUseAssociationRules) {
      approximateCindCandidates.withBroadcastSet(frequentConditionDataSets.associationRules,
        CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST)
    }
    if (parameters.isFindFrequentCaptures) {
      approximateCindCandidates.withBroadcastSet(frequentCapturesBloomFilter,
      CreateDependencyCandidates.FREQUENT_CAPTURES_BROADCAST)
    }

    // Merge the CINDs with the Bloom filters.
    val approximateCinds = approximateCindCandidates
      // BUG: allowing this code makes Flink crash (at least locally)
      //      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      //      .combineGroup(new IntersectCindCandidatesApproximate())
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .reduceGroup(new IntersectHalfApproximateCindCandidates(bloomFilterParameters, parameters.mergeWindowSize))
      .name("Intersect half approximated CIND evidences")
      .filter { cindSet =>
      cindSet.depCount >= minSupport && // CIND set is frequent.
        (cindSet.approximateRefConditions.length > 0 || cindSet.refConditions.length > 0) // CIND set is not empty.
    }.name("Filter frequent half approximated CIND sets")

    val firstRoundCindSets = approximateCinds
      .flatMap { (approximateCinds: HalfApproximateCindSet, out: Collector[CindSet]) =>
      if (approximateCinds.isExact) {
        out.collect(approximateCinds.toCindSet)
      }
    }.name("Filter first-round CIND sets")

    val bloomFilterApproximateCinds = approximateCinds
      .filter { halfApproximateCindSet =>
      !halfApproximateCindSet.isExact && halfApproximateCindSet.approximateRefConditions.length > 0
    }.name("Filter Bloom filter approximated CIND sets")

    val explicitApproximateCinds = approximateCinds
      .filter { halfApproximateCindSet =>
      !halfApproximateCindSet.isExact && halfApproximateCindSet.approximateRefConditions.length == 0
    }.name("Filter explicit approximated CIND sets")

    val secondRoundCandidates = tripleJoin
      .flatMap(new CreateApproximatedCindCandidates(bloomFilterParameters, exactnessThreshold,
      parameters.isUseAssociationRules, splitStrategy = parameters.rebalanceSplitStrategy))
      .withBroadcastSet(explicitApproximateCinds, CreateApproximatedCindCandidates.EXACT_CIND_CANDIDATES)
      .withBroadcastSet(bloomFilterApproximateCinds, CreateApproximatedCindCandidates.BLOOM_FILTER_CIND_CANDIDATES)
      .name("Create second-round CIND evidences")
    if (parameters.isUseAssociationRules) {
      secondRoundCandidates.withBroadcastSet(frequentConditionDataSets.associationRules,
        CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST)
    }

    val secondRoundCindSets = secondRoundCandidates
      // We set it combinable, so this right here should not be necessary.
      //      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      //      .combineGroup(new IntersectCindCandidates(countMergeFunction = ApproximateAllAtOnceTraversalStrategy.assertEqualsAndFirst))
      //      .name("Intersect second-round CIND evidences")
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .reduceGroup(new IntersectCindCandidates(parameters.mergeWindowSize,
      countMergeFunction = ApproximateAllAtOnceTraversalStrategy.assertEqualsAndFirst))
      .name("Intersect second-round CIND evidences")

    splitAndCleanCindSets(parameters, firstRoundCindSets, secondRoundCindSets)
  }

  /**
   * @return whether this strategy can incorporate frequent conditions.
   */
  override def isAbleToUseFrequentConditions: Boolean = true

  override def extractJoinLineSize: (JoinLine) => Int = _.numCombinedConditions

  override def extractMaximumJoinLineSize: (JoinLineLoad) => Int = _.combinedBlockSize
}

object ApproximateAllAtOnceTraversalStrategy {

  def assertEqualsAndFirst(x: Int, y: Int): Int = {
    if (x != y && y != 0)
      throw new IllegalStateException(s"Encountered illegal different values $x and $y.")
    x
  }

}