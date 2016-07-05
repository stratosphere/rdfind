package de.hpi.isg.sodap.rdfind.plan

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators._
import de.hpi.isg.sodap.rdfind.operators.candidate_extraction.{ExtractBinaryBinaryCindCandidates, ExtractUnaryBinaryCindCandidates}
import de.hpi.isg.sodap.rdfind.operators.candidate_generation.{GenerateBinaryBinaryCindCandidates, GenerateBinaryUnaryCindCandidates, GenerateUnaryBinaryCindCandidates}
import de.hpi.isg.sodap.rdfind.operators.candidate_merging.{IntersectCindCandidates, MultiunionHalfApproximateOverlapCandidates, MultiunionOverlapCandidates}
import de.hpi.isg.sodap.rdfind.plan.FrequentConditionPlanner.ConstructedDataSets
import de.hpi.isg.sodap.rdfind.plan.SmallToLargeTraversalStrategy.IntermediateOverlap
import de.hpi.isg.sodap.rdfind.programs.RDFind
import de.hpi.isg.sodap.rdfind.programs.RDFind._
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import de.hpi.isg.sodap.rdfind.util.{BloomFilterParameters, ConditionCodes, UntypedBloomFilterParameters}
import org.apache.flink.api.java.io.PrintingOutputFormat
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.runtime.ScalaRunTime

/**
 * @author Sebastian
 * @since 14.04.2015.
 */
class SmallToLargeTraversalStrategy(baseBloomFilterParameters: UntypedBloomFilterParameters)
  extends TraversalStrategy {

  private val cindBloomFilterParameters = baseBloomFilterParameters.withType[Cind](classOf[Cind.Funnel].getName)

  /**
   * Builds a Flink subplan, that extracts CINDs from joined RDF triples.
   * @param tripleJoin contains all co-occurring, frequent conditions from the RDF data set
   * @param frequentConditionDataSets contains frequent conditions, Bloom filters, and association rules
   * @param parameters is the configuration of the RDFind job
   * @return a [[DataSet]] containing all discovered CINDs
   */
  override def enhanceFlinkPlan(tripleJoin: DataSet[JoinLine],
                                frequentConditionDataSets: ConstructedDataSets,
                                frequentCapturesBloomFilter: DataSet[BloomFilter[Condition]],
                                parameters: Parameters): DataSet[Cind] = {

    if (frequentCapturesBloomFilter != null) {
      throw new IllegalArgumentException("Frequent capture Bloom filters are not supported in this traversal strategy.")
    }

    val minSupport = parameters.minSupport

    if (parameters.rebalanceSplitStrategy != 1) throw new IllegalArgumentException("Only split strategy 1 is supported!")


    if (parameters.isUseFrequentItemSet && parameters.isCreateAnyBinaryCaptures) {
      throw new IllegalStateException("Lazy evaluation of frequency of binary captures is not supported in S2L traversal.")
    }

    // Find frequent overlaps of unary captures.
    val pairwiseOverlaps = findFrequentSingleSingleConditionOverlapsExt(tripleJoin, frequentConditionDataSets, parameters)

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      pairwiseOverlaps.map(x => s"Overlap: ${x.overlap}x ${x.smallConditionCount} ~ ${x.largeConditionCount}").output(new PrintingOutputFormat())

    // Extract unary/unary CINDs from the pairwise overlaps.
    var singleSingleCinds = pairwiseOverlaps.flatMap { (pairwiseOverlap: PairwiseOverlap, out: Collector[Cind]) =>
      if (pairwiseOverlap.smallConditionCount.count == pairwiseOverlap.overlap) {
        out.collect(Cind(pairwiseOverlap.smallConditionCount.captureType,
          pairwiseOverlap.smallConditionCount.conditionValue1NotNull, "",
          pairwiseOverlap.largeConditionCount.captureType,
          pairwiseOverlap.largeConditionCount.conditionValue1NotNull, "",
          pairwiseOverlap.overlap))
      }
      if (pairwiseOverlap.largeConditionCount.count == pairwiseOverlap.overlap) {
        out.collect(Cind(pairwiseOverlap.largeConditionCount.captureType,
          pairwiseOverlap.largeConditionCount.conditionValue1NotNull, "",
          pairwiseOverlap.smallConditionCount.captureType,
          pairwiseOverlap.smallConditionCount.conditionValue1NotNull, "",
          pairwiseOverlap.overlap))
      }
    }.name("Extract 1/1 CINDs")

    if (parameters.isUseAssociationRules) {
      // Since we must not use association rules when calculating overlaps (they are undirected!), we filter here.
      singleSingleCinds = singleSingleCinds
        .filter(new FilterAssociationRuleImpliedCinds)
        .withBroadcastSet(frequentConditionDataSets.associationRules, FilterAssociationRuleImpliedCinds.ASSOCIATION_RULE_BROADCAST)
        .name("Filter association-rule-implied CINDs")

    }

    // Extract the proper proper unary/unary overlaps.
    val singleSingleProperOverlaps = pairwiseOverlaps.flatMap { (pairwiseOverlap: PairwiseOverlap, out: Collector[Cind]) =>
      if (pairwiseOverlap.smallConditionCount.count != pairwiseOverlap.overlap) {
        out.collect(Cind(pairwiseOverlap.smallConditionCount.captureType,
          pairwiseOverlap.smallConditionCount.conditionValue1NotNull, "",
          pairwiseOverlap.largeConditionCount.captureType,
          pairwiseOverlap.largeConditionCount.conditionValue1NotNull, "",
          pairwiseOverlap.overlap))
      }
      if (pairwiseOverlap.largeConditionCount.count != pairwiseOverlap.overlap) {
        out.collect(Cind(pairwiseOverlap.largeConditionCount.captureType,
          pairwiseOverlap.largeConditionCount.conditionValue1NotNull, "",
          pairwiseOverlap.smallConditionCount.captureType,
          pairwiseOverlap.smallConditionCount.conditionValue1NotNull, "",
          pairwiseOverlap.overlap))
      }
    }.name("Extract proper 1/1 overlaps")
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      singleSingleCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x 1/1-CINDs.").output(new PrintingOutputFormat())
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      singleSingleCinds.map(x => s"1/1-CIND: $x").output(new PrintingOutputFormat())


    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      singleSingleProperOverlaps.map(x => 1).reduce(_ + _).map(x => s"Found $x 1/1-PINDs.").output(new PrintingOutputFormat())
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      singleSingleProperOverlaps.map(x => s"1/1-PIND: $x").output(new PrintingOutputFormat())

    // Find the 1/2 CINDs and use the 1/1 CINDs to prune.
    val singleDoubleCindSets =
      findSingleDoubleCinds(tripleJoin, frequentConditionDataSets, singleSingleCinds, parameters)
    val singleDoubleCinds = singleDoubleCindSets
      .flatMap { (singleDoubleCindSet: CindSet, out: Collector[Cind]) =>
      singleDoubleCindSet.refConditions.foreach { refCondition =>
        out.collect(Cind(singleDoubleCindSet.depCaptureType, singleDoubleCindSet.depConditionValue1, "",
          refCondition.conditionType, refCondition.conditionValue1NotNull, refCondition.conditionValue2NotNull,
          singleDoubleCindSet.depCount))
      }
    }.name("Split 1/2 CIND sets")

    // DEBUG print:
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      singleDoubleCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x 1/2-CINDs.").output(new PrintingOutputFormat())
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      singleDoubleCindSets.map(x => s"1/2-CIND: $x").output(new PrintingOutputFormat())

    val doubleSingleCindSets = findDoubleSingleCindSets(tripleJoin, singleSingleProperOverlaps, frequentConditionDataSets,
      minSupport)
    val doubleSingleCinds = doubleSingleCindSets
      .flatMap { (doubleSingleCindSet: CindSet, out: Collector[Cind]) =>
      doubleSingleCindSet.refConditions.foreach { refCondition =>
        out.collect(Cind(doubleSingleCindSet.depCaptureType, doubleSingleCindSet.depConditionValue1,
          doubleSingleCindSet.depConditionValue2,
          refCondition.conditionType, refCondition.conditionValue1NotNull, refCondition.conditionValue2NotNull,
          doubleSingleCindSet.depCount))
      }
    }.name("Split 2/1 CIND sets")

    // DEBUG print:
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      doubleSingleCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x 2/1-CINDs.").output(new PrintingOutputFormat())
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      doubleSingleCinds.map(x => s"2/1-CIND: $x").output(new PrintingOutputFormat())

    val doubleDoubleCindSets = findDoubleDoubleCindSets(tripleJoin, singleSingleCinds, singleSingleProperOverlaps,
      doubleSingleCinds, singleDoubleCinds, frequentConditionDataSets.frequentDoubleConditions,
      frequentConditionDataSets.frequentDoubleConditionBloomFilter, parameters)

    val doubleDoubleCinds = doubleDoubleCindSets.flatMap(_.split).name("Split 2/2 CIND sets") //.map(_.toCind)

    // Remove redundant CINDs if requested.
    val allCinds =
      if (parameters.isCleanImplied) {
        removeImpliedCinds(singleSingleCinds, singleDoubleCinds, doubleSingleCinds, doubleDoubleCinds, parameters)
      } else {
        singleSingleCinds
          .union(singleDoubleCinds)
          .union(doubleSingleCinds)
          .union(doubleDoubleCinds)
      }

    allCinds
  }


  /**
   * Finds 1/1 overlaps by approximating the result at first using spectral Bloom filters in a degrade-gracefully
   * manner. Then in a second round, unclear overlaps are reevaluated.
   */
  def findOverlapsViaApproximation(tripleJoin: DataSet[JoinLine],
                                   frequentConditionDataSets: FrequentConditionPlanner.ConstructedDataSets,
                                   parameters: Parameters) = {
    val bitsPerInt =
    // Determine the necessary bits to encode the required min support.
      if (parameters.spectralBloomFilterBits == -1) {
        val result = 33 - Integer.numberOfLeadingZeros(parameters.minSupport)
        LoggerFactory.getLogger(getClass).info(s"Using $result bits per entry in the spectral Bloom filter.")
        result
      } else parameters.spectralBloomFilterBits

    // Make sure that we do not miscalculation.
    if ((1 << bitsPerInt) - 1 < parameters.minSupport) {
      throw new IllegalStateException(s"Chose $bitsPerInt bits for the spectral Bloom filter and" +
        s" ${parameters.minSupport} as max count - that seems to be insufficient.")
    }

    // Set up configuration. We use the same values as for the ApproximateAllAtOnceTraversalStrategy.
    val bloomFilterParameters = BloomFilterParameters[ConditionCount](
      parameters.explicitCandidateThreshold * 10, 0.1, (new ConditionCount.NoCountFunnel).getClass.getName, bitsPerInt)
    val minSupport = parameters.minSupport

    val haUnaryUnaryOverlapCandidates = tripleJoin
      .flatMap(new ExtractBalancedHalfApproximateUnaryUnaryOverlapCandidates(parameters.explicitCandidateThreshold,
      bloomFilterParameters, /*parameters.isUseAssociationRules*/ false))
      .name("Extract balanced half-approximate 1/1 overlap evidences")
//    if (parameters.isUseAssociationRules) {
//      haUnaryUnaryOverlapCandidates.withBroadcastSet(frequentConditionDataSets.associationRules,
//        CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST)
//    }

    val halfApproximateOverlaps = haUnaryUnaryOverlapCandidates
      .groupBy("lhsCaptureType", "lhsConditionValue1")
      .reduceGroup(new MultiunionHalfApproximateOverlapCandidates(bloomFilterParameters, parameters.explicitCandidateThreshold,
      parameters.mergeWindowSize))
      .name("Union all half-approximate 1/1 overlap evidences")
      .filter(_.depCount >= minSupport)
      .name("Filter half-approximate overlaps w/ infrequent LHS")
      .flatMap(new EvaluateHalfApproximateOverlapSets(bloomFilterParameters, minSupport))
      .name("Evaluate half-approximate overlaps")

    val firstRoundOverlapSets = halfApproximateOverlaps
      .filter(_.lhsCount > 0) // We use that criterion to mark approximate overlap sets.
      .name("Remove approximate overlaps")
      .map(halfApproximateOverlaps => halfApproximateOverlaps.toOverlapSet)
      .name("Transform half-approximate overlap sets")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      firstRoundOverlapSets.map(x => s"First round overlap: $x").printOnTaskManager("TRACE")

    val approximateOverlapSets = halfApproximateOverlaps
      .filter(_.lhsCount < 0)
      .name("Remove exact overlaps")

    val reducedBalancedUnaryUnaryOverlapCandidates = tripleJoin
      .flatMap(new CreateReducedBalancedUnaryUnaryOverlapCandidates(bloomFilterParameters,
      parameters.explicitCandidateThreshold, /*parameters.isUseAssociationRules*/ false))
      .name("Create second-round overlap evidences")
      .withBroadcastSet(approximateOverlapSets,
        CreateReducedBalancedUnaryUnaryOverlapCandidates.APPROXIMATE_OVERLAPS_BROADCAST)
//    if (parameters.isUseAssociationRules) {
//      reducedBalancedUnaryUnaryOverlapCandidates.withBroadcastSet(frequentConditionDataSets.associationRules,
//        CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST)
//    }

    // Create and merge the overlap candidates.
    val secondRoundOverlapSets = reducedBalancedUnaryUnaryOverlapCandidates
      .groupBy("lhsCaptureType", "lhsConditionValue1")
      .reduceGroup(new MultiunionOverlapCandidates())
      .name("Merge 1/1 overlap evidences")

    val frequentSecondRoundOverlapSets = secondRoundOverlapSets.flatMap { (overlapSet: OverlapSet, out: Collector[OverlapSet]) =>
      if (overlapSet.lhsCount >= minSupport) {
        overlapSet.rhsConditions = overlapSet.rhsConditions.filter(_.count >= minSupport)
        out.collect(overlapSet)
      }
    }.name("Filter infrequent 1/1 overlaps")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      frequentSecondRoundOverlapSets.map(x => s"Second round overlap: $x").printOnTaskManager("TRACE")

    firstRoundOverlapSets.union(frequentSecondRoundOverlapSets).map(x => x).name("Forward")
  }


  /**
   * Finds frequent 1/1 overlaps by searching directly for any possible candidate overlap.
   */
  def findFrequentUnaryUnaryOverlapsDirectly(tripleJoin: DataSet[JoinLine],
                                             frequentConditionDataSets: FrequentConditionPlanner.ConstructedDataSets,
                                             parameters: Parameters): DataSet[OverlapSet] = {
    val minSupport = parameters.minSupport

    // Create unary/unary overlap candidates.
    val overlapCandidates = if (parameters.isBalanceOverlapCandidates)
      tripleJoin
        .flatMap(new CreateBalancedUnaryUnaryOverlapCandidates(/*parameters.isUseAssociationRules*/ false))
        .name("Create 1/1 balanced overlap evidences")
    else
      tripleJoin
        .flatMap(new CreateUnaryUnaryOverlapCandidates(/*parameters.isUseAssociationRules*/ false))
        .name("Create 1/1 overlap evidences")
//    if (parameters.isUseAssociationRules) {
//      overlapCandidates.withBroadcastSet(frequentConditionDataSets.associationRules,
//        CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST)
//    }

    // Merge the overlap candidates.
    val overlaps = overlapCandidates
      .groupBy("lhsCaptureType", "lhsConditionValue1")
      .reduceGroup(new MultiunionOverlapCandidates())
      .name("Merge 1/1 overlap evidences")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      overlaps
        .map(x => s"Normal overlaps: ${x.lhsCount}x${ConditionCodes.prettyPrint(x.lhsCaptureType, x.lhsConditionValue1)} -> ${ScalaRunTime.stringOf(x.rhsConditions)}")
        .output(new PrintingOutputFormat())


    // In the following, we are only interested in overlaps where
    // (i) the left-hand side is frequent (it is in fact the distinct value count of this capture)
    // (ii) right-hand sides that are frequent (only those can lead to support CINDs)
    val frequentOverlaps = overlaps.flatMap { (overlapSet: OverlapSet, out: Collector[OverlapSet]) =>
      if (overlapSet.lhsCount >= minSupport) {
        overlapSet.rhsConditions = overlapSet.rhsConditions.filter(_.count >= minSupport)
        out.collect(overlapSet)
      }
    }.name("Filter infrequent 1/1 overlaps")
    frequentOverlaps
  }


  /**
   * Finds all frequent overlaps of unary captures.
   * @param tripleJoin contains the join of triples
   * @param parameters are the RDFind parameters
   * @return a [[DataSet]] with the frequent overlaps
   */
  private def findFrequentSingleSingleConditionOverlapsExt(tripleJoin: DataSet[JoinLine],
                                                           frequentConditionDataSets: FrequentConditionPlanner.ConstructedDataSets,
                                                           parameters: Parameters):
  DataSet[PairwiseOverlap] = {

    // At first, create overlap sets (directed, i.e., avoid calculating overlaps twice; use symmetry instead)
    val frequentOverlaps: DataSet[OverlapSet] =
      if (parameters.explicitCandidateThreshold != -1)
        findOverlapsViaApproximation(tripleJoin, frequentConditionDataSets, parameters)
      else
        findFrequentUnaryUnaryOverlapsDirectly(tripleJoin, frequentConditionDataSets, parameters)

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      frequentOverlaps
        .map(x => s"Frequent overlaps: ${x.lhsCount}x ${x.depCondition} -> ${ScalaRunTime.stringOf(x.rhsConditions)}")
        .output(new PrintingOutputFormat())

    // Filter out the distinct values from the overlaps. They are already frequent.
    val distinctValues = frequentOverlaps.map { overlapSet =>
      ConditionCount(overlapSet.lhsCaptureType, overlapSet.lhsConditionValue1, null, overlapSet.lhsCount)
    }.name("Extract distinct values from 1/1 overlap set LHS")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      distinctValues
        .map(x => s"Distinct values: $x")
        .output(new PrintingOutputFormat())

    // Split the overlap sets into single overlaps. Furthermore, the right-hand
    // side will be ordered after the left-hand side (symmetric overlaps).
    val intermediateOverlaps = frequentOverlaps.flatMap { (overlapSet: OverlapSet, out: Collector[IntermediateOverlap]) =>
      val intermediateOverlap = IntermediateOverlap(
        ConditionCount(overlapSet.lhsCaptureType, overlapSet.lhsConditionValue1, overlapSet.lhsConditionValue2, overlapSet.lhsCount),
        null, 0)
      overlapSet.rhsConditions.foreach { depConditionCount =>
        intermediateOverlap.secondCondition = depConditionCount.toCondition
        intermediateOverlap.overlap = depConditionCount.count
        out.collect(intermediateOverlap)
      }
    }.name("Split 1/1 overlap sets")

    // Join the distinct value count of the right-hand side capture to the overlaps, so that we can compensate for the
    // fact that we have directed overlaps.
    val pairwiseOverlaps = intermediateOverlaps
      .joinWithTiny(distinctValues)
      .where("secondCondition.conditionType", "secondCondition.conditionValue1")
      .equalTo("captureType", "conditionValue1") { (intermediateOverlap: IntermediateOverlap, distinctValue: ConditionCount) =>
      PairwiseOverlap(intermediateOverlap.firstCondition, distinctValue, intermediateOverlap.overlap)
    }.name("Join overlaps with distinct values")

    pairwiseOverlaps
  }

  private def findSingleDoubleCinds(tripleJoin: DataSet[JoinLine], frequentConditionDataSets: ConstructedDataSets,
                                    singleSingleInds: DataSet[Cind],
                                    parameters: Parameters): DataSet[CindSet] = {

    // Find 1/2 candidates by joining 1/1 CINDs on their left-hand side.
    val singleDoubleIndCandidates = singleSingleInds
      .groupBy("depCaptureType", "depConditionValue1")
      .reduceGroup(new GenerateUnaryBinaryCindCandidates)
      .name("Generate 1/2 CIND candidates")

    val bloomFilterParameters = this.cindBloomFilterParameters

    // Build a Bloom filter over the 1/2 candidates.
    val singleDoubleIndCandidateBloomFilter = singleDoubleIndCandidates
      .mapPartition {
      (singleDoubleCinds: Iterator[Cind], out: Collector[BloomFilter[Cind]]) =>

        // TODO: Think about how to optimize these parameters... maybe estimate from file size?
        val bloomFilter = bloomFilterParameters.createBloomFilter
        while (singleDoubleCinds.hasNext) {
          val singleDoubleCindCandidate = singleDoubleCinds.next()
          bloomFilter.put(singleDoubleCindCandidate)
        }

        out.collect(bloomFilter)
    }.name("Create partial 1/2 CIND canidate Bloom filters")
      .reduceGroup {
      (iterator: Iterator[BloomFilter[Cind]], out: Collector[BloomFilter[Cind]]) =>
        val bloomFilter = iterator.next()
        while (iterator.hasNext) {
          bloomFilter.putAll(iterator.next())
        }
        out.collect(bloomFilter)
    }.name("Merge partial 1/2 CIND candidate Bloom filters")

    // Create the actual 2/1 CIND candidates using the Bloom filter to prune early on.
    val singleDoubleCindCandidates = tripleJoin
      .flatMap(new ExtractUnaryBinaryCindCandidates(isTestBinaryCaptureFrequency = false))
      .withBroadcastSet(singleDoubleIndCandidateBloomFilter,
        ExtractUnaryBinaryCindCandidates.CANDIDATE_BLOOM_FILTER_BROADCAST)
      .name("Extract 1/2 CIND evidences")

    // Consolidate the candidates to the actual 2/1 CIND candidates.
    val minSupport = parameters.minSupport
    val singleDoubleCinds = singleDoubleCindCandidates
      .groupBy("depCaptureType", "depConditionValue1")
      .combineGroup(new IntersectCindCandidates)
      .name("Intersect 1/2 CIND evidences")
      .groupBy("depCaptureType", "depConditionValue1")
      .reduceGroup(new IntersectCindCandidates)
      .name("Intersect 1/2 CIND evidences")
      .filter(_.refConditions.length > 0)
      .name("Remove empty CIND sets")
      .filter(_.depCount >= minSupport)
      .name("Remove infrequent CIND sets")
    singleDoubleCinds
  }

  /**
   * Find overlaps of two-captures with one-captures that might
   * @param tripleJoin
   * @param singleSingleProperOverlaps
   * @param constructedDataSets
   * @param minSupport
   * @return
   */
  private def findDoubleSingleCindSets(tripleJoin: DataSet[JoinLine],
                                       singleSingleProperOverlaps: DataSet[Cind],
                                       constructedDataSets: ConstructedDataSets,
                                       minSupport: Int): DataSet[CindSet] = {

    // Find minimal CINDs using a Bloom filter for candidates.
    val doubleSingleCindCandidates = singleSingleProperOverlaps
      .groupBy("refCaptureType", "refConditionValue1")
      .reduceGroup(new GenerateBinaryUnaryCindCandidates)
      .name("Generate 2/1 CIND candidates")

    val bloomFilterParameters = this.cindBloomFilterParameters

    // Create a Bloom filter for the overlaps.
    // IDEA: Create Bloom filters per CIND type (e.g., s[p,o]<s[p], s[p,o]<s[p], ...)
    // This could, however, also be controlled via the Bloom filter size.
    val doubleSingleCindCandidatefilter = doubleSingleCindCandidates
      .mapPartition {
      (doubleSingleOverlapCandidates: Iterator[Cind], out: Collector[BloomFilter[Cind]]) =>

        // TODO: Think about how to optimize these parameters... maybe estimate from file size?
        val bloomFilter = bloomFilterParameters.createBloomFilter
        while (doubleSingleOverlapCandidates.hasNext) {
          val doubleSingleOverlapCandidate = doubleSingleOverlapCandidates.next()
          bloomFilter.put(doubleSingleOverlapCandidate)
        }

        out.collect(bloomFilter)
    }.name("Create partial 2/1 CIND canidate Bloom filters")
      .reduceGroup {
      (iterator: Iterator[BloomFilter[Cind]], out: Collector[BloomFilter[Cind]]) =>
        val bloomFilter = iterator.next()
        while (iterator.hasNext) {
          bloomFilter.putAll(iterator.next())
        }
        out.collect(bloomFilter)
    }.name("Merge partial 2/1 CIND canidate Bloom filters")

    // Create overlap occurrences.
    val localDoubleSingleCindSets = tripleJoin
      .flatMap(new CreateBinaryUnaryCindCandidates(isTestBinaryCaptureFrequency = false))
      .withBroadcastSet(doubleSingleCindCandidatefilter,
        CreateBinaryUnaryCindCandidates.CANDIDATE_BLOOM_FILTER_BROADCAST)
      .name("Extract 2/1 CIND evidences")

    val doubleSingleCindSets = localDoubleSingleCindSets
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .combineGroup(new IntersectCindCandidates())
      .name("Intersect 2/1 CIND evidences")
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .reduceGroup(new IntersectCindCandidates())
      .name("Intersect 2/1 CIND evidences")
      .filter(_.depCount >= minSupport)
      .name("Remove infrequent 2/1 CIND sets")
      .filter(_.refConditions.length > 0)
      .name("Remove empty 2/1 CIND sets")

    doubleSingleCindSets
  }

  /**
   * This method finds 2/2-CINDs.
   */
  private def findDoubleDoubleCindSets(tripleJoin: DataSet[JoinLine],
                                       singleSingleCinds: DataSet[Cind],
                                       singleSingleProperOverlaps: DataSet[Cind],
                                       doubleSingleCinds: DataSet[Cind],
                                       singleDoubleCinds: DataSet[Cind],
                                       frequentDoubleConditions: DataSet[DoubleConditionCount],
                                       frequentDoubleConditionBloomFilters: DataSet[(Int, BloomFilter[Condition])],
                                       parameters: Parameters):
  DataSet[CindSet] = {

    // a) according 2/1 CINDs must exist (minimal 2/1 CINDs and inferred 2/1 CINDs from 1/1 CINDs and 1/1 FDVOs)
    // b) there must be no according 1/2 CINDs (because of minimality)
    // c) In principle, we could think of using 1/2 DVOs, but we don't have those


    // At first, infer non-mininmal 2/1-CINDs.

    // Aggregate them to infer non-minimal 2/1-CINDs.
    // Little hack: support = {0 => cind, 1 => overlap}
    val inferredDoubleSingleCinds = singleSingleCinds.map({
      cind =>
        cind.support = 0
        cind
    }).name("Mark 1/1 CINDs")
      .union(singleSingleProperOverlaps.map({
      cind =>
        cind.support = 1
        cind
    }).name("Mark 1/1 proper overlaps"))
      .groupBy("refCaptureType", "refConditionValue1")
      .reduceGroup(new InferDoubleSingleCinds)
      .name("Infer non-minimal 2/1 CINDs")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      inferredDoubleSingleCinds.map(x => s"Inferred 2/1-CIND $x.").output(new PrintingOutputFormat())

    // IDEA: Do not alter this data set but use condition codes for the inferred dependencies.
    val frequentCaptures = frequentDoubleConditions
      .map { condition =>
      condition.conditionType = addSecondaryConditions(condition.conditionType)
      condition
    }.name("Add projection attribute")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      frequentCaptures.map(x => 1).reduce(_ + _).map(x => s"Found $x frequent binary captures").output(new PrintingOutputFormat[String]())

    // Filter non-frequent 2/1-CINDs.
    val frequentInferredDoubleSingleCinds = inferredDoubleSingleCinds
      .join(frequentCaptures)
      .where("depCaptureType", "depConditionValue1", "depConditionValue2")
      .equalTo("conditionType", "value1", "value2") { (cind, frequentCondition) => cind }
      .name("Remove infrequent inferred 2/1 CINDs")

    // DEBUG print
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      frequentInferredDoubleSingleCinds.map(x => 1).reduce(_ + _).map(x => s"Inferred $x frequent 2/1-CINDs.").output(new PrintingOutputFormat())

    // Generate 2/2-CIND candidates from the 2/1-CINDs.
    val allDoubleSingleCinds = doubleSingleCinds
      .union(frequentInferredDoubleSingleCinds)
      .map(x => x).name("Forward") // XXX Does this help?

    val nonMinimalDoubleDoubleCindCandidates = allDoubleSingleCinds
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .reduceGroup(new GenerateBinaryBinaryCindCandidates)
      .name("Generate 2/2 CIND candidates (from 2/1 CINDs)")


    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      nonMinimalDoubleDoubleCindCandidates.map(x => 1).reduce(_ + _).map(x => s"Created $x 2/2-CIND candidates.").output(new PrintingOutputFormat())

    // Prune non-minimal 2/2-CIND candidates using the 1/2-CINDs.
    val minimalDoubleDoubleCindCandidates = nonMinimalDoubleDoubleCindCandidates
      .coGroup(singleDoubleCinds)
      .where("refCaptureType", "refConditionValue1", "refConditionValue2")
      .equalTo("refCaptureType", "refConditionValue1", "refConditionValue2")
      .apply(new PruneNonMinimalDoubleDoubleCindCandidates)
      .name("Prunte 2/2 CIND candidates (from 1/2 CINDs)")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      minimalDoubleDoubleCindCandidates.map(x => 1).reduce(_ + _).map(x => s"Created $x minimal 2/2-CIND candidates.").output(new PrintingOutputFormat())

    val bloomFilterParameters = this.cindBloomFilterParameters

    // Put the candidates into a Bloom filter.
    val doubleDoubleCindCandidatesBloomFilter = minimalDoubleDoubleCindCandidates
      .mapPartition {
      (cindCandidates: Iterator[Cind], out: Collector[BloomFilter[Cind]]) =>

        // TODO: Think about how to optimize these parameters... maybe estimate from file size?
        val bloomFilter = bloomFilterParameters.createBloomFilter
        while (cindCandidates.hasNext) {
          val cindCandidate = cindCandidates.next()
          bloomFilter.put(cindCandidate)
        }

        out.collect(bloomFilter)
    }.name("Create partial 2/2 CIND candidate Bloom filters")
      .reduceGroup {
      (iterator: Iterator[BloomFilter[Cind]], out: Collector[BloomFilter[Cind]]) =>
        val bloomFilter = iterator.next()
        while (iterator.hasNext) {
          bloomFilter.putAll(iterator.next())
        }

        println(s"Bloom filter for 2/2 CIND candidates has an expected FPP of ${bloomFilter.expectedFpp() * 100}%.")

        out.collect(bloomFilter)
    }.name("Merge partial 2/2 CIND candidate Bloom filters")

    // Create the 2/2 CIND-sets.
    val localDoubleDoubleCinds = tripleJoin
      .flatMap(new ExtractBinaryBinaryCindCandidates(false))
      .withBroadcastSet(doubleDoubleCindCandidatesBloomFilter,
        ExtractBinaryBinaryCindCandidates.CANDIDATE_BLOOM_FILTER_BROADCAST)
      .name("Extract 2/2 CIND evidences")

    // Consolidate the local 2/2 CIND sets.
    val minSupport = parameters.minSupport
    val doubleDoubleCindSets = localDoubleDoubleCinds
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .combineGroup(new IntersectCindCandidates())
      .name("Intersect 2/2 CIND evidences")
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .reduceGroup(new IntersectCindCandidates())
      .name("Intersect 2/2 CIND evidences")
      .filter(_.depCount >= minSupport)
      .name("Remove infrequent 2/2 CIND sets")
      .filter(_.refConditions.nonEmpty)
      .name("Remove empty 2/2 CIND sets")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      doubleDoubleCindSets.map(x => 1).reduce(_ + _).map(x => s"Found $x 2/2-CINDs.").output(new PrintingOutputFormat())
    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE)
      doubleDoubleCindSets.map(x => s"2/2-CIND: $x").output(new PrintingOutputFormat())

    doubleDoubleCindSets
  }

  /**
   * @return whether this strategy can incorporate frequent conditions.
   */
  override def isAbleToUseFrequentConditions: Boolean = true


  override def extractJoinLineSize: (JoinLine) => Int = _.numUnaryConditions

  override def extractMaximumJoinLineSize: (JoinLineLoad) => Int = _.unaryBlockSize
}


object SmallToLargeTraversalStrategy {

  /**
   * This class represents the overlap of two captures.
   * @param firstCondition is a capture whose distinct value count is known
   * @param secondCondition is another captures
   * @param overlap is the number of overlapping distinct values of the two captures
   */
  case class IntermediateOverlap(var firstCondition: ConditionCount, var secondCondition: Condition, var overlap: Int)


}