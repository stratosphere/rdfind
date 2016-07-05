package de.hpi.isg.sodap.rdfind.plan

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.{CountItemsUsingMap, RemoveNonMinimalDoubleXxxCinds, RemoveNonMinimalXxxSingleCinds}
import de.hpi.isg.sodap.rdfind.programs.RDFind
import de.hpi.isg.sodap.rdfind.programs.RDFind.Parameters
import de.hpi.isg.sodap.rdfind.util.ConditionCodes
import org.apache.flink.api.java.io.PrintingOutputFormat
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector

/**
 * This trait describes a strategy to traverse the RDFind search space.
 *
 * @author Sebastian
 * @since 10.04.2015.
 */
trait TraversalStrategy {

  /**
   * Builds a Flink subplan, that extracts CINDs from joined RDF triples.
   * @param tripleJoin contains all co-occurring, frequent conditions from the RDF data set
   * @param frequentConditionDataSets contains frequent conditions, Bloom filters, and association rules
   * @param parameters is the configuration of the RDFind job
   * @return TODO
   */
  def enhanceFlinkPlan(tripleJoin: DataSet[JoinLine],
                       frequentConditionDataSets: FrequentConditionPlanner.ConstructedDataSets,
                       //                       averageJoinLineLoad: DataSet[JoinLineLoad],
                       frequentCapturesBloomFilter: DataSet[BloomFilter[Condition]],
                       parameters: RDFind.Parameters)
  : DataSet[Cind]

  /**
   * @return whether this strategy can incorporate frequent conditions.
   */
  def isAbleToUseFrequentConditions: Boolean

  /**
   * Splits the [[CindSet]]s into [[Cind]]s and optionally removes non-minimal CINDs.
   * @param cindSets
   * @return
   */
  def splitAndCleanCindSets(parameters: Parameters, cindSets: DataSet[CindSet]*) = {

    // Transform the candidates into actual CINDs.
    // NB: Due to a limitation in Flink, we have to unpack the CINDs first and then union them.
    var allCinds = cindSets.map(_.flatMap { (cindSet: CindSet, out: Collector[Cind]) =>
      cindSet.refConditions.foreach { refCondition =>
        out.collect(Cind(cindSet.depCaptureType,
          cindSet.depConditionValue1,
          cindSet.depConditionValue2,
          refCondition.conditionType,
          refCondition.conditionValue1NotNull,
          refCondition.conditionValue2NotNull,
          cindSet.depCount))
      }
    }.name("Split CIND sets")).reduceLeft(_.union(_))
      .map(cind => cind)
      //      .withForwardedFields("depCaptureType", "depConditionValue1", "depConditionValue2", "refCaptureType", "refConditionValue1", "refConditionValue2")
      .name("Forward")

    if (parameters.counterLevel >= 1)
      allCinds = allCinds.map(new CountItemsUsingMap[Cind](RDFind.BROAD_CIND_COUNT_ACCUMULATOR))

    // XXX: Does this help?
    //      .map { x =>
    //      x.checkSanity()
    //      x
    //    }

    val singleSingleCinds = allCinds
      .filter { cind =>
      ConditionCodes.isUnaryCondition(cind.depCaptureType) && ConditionCodes.isUnaryCondition(cind.refCaptureType)
    }.name("Filter 1/1 CINDs")

    val singleDoubleCinds = allCinds
      .filter { cind =>
      ConditionCodes.isUnaryCondition(cind.depCaptureType) && ConditionCodes.isBinaryCondition(cind.refCaptureType)
    }.name("Filter 1/2 CINDs")

    val doubleSingleCinds = allCinds
      .filter { cind =>
      ConditionCodes.isBinaryCondition(cind.depCaptureType) && ConditionCodes.isUnaryCondition(cind.refCaptureType)
    }.name("Filter 2/1 CINDs")

    val doubleDoubleCinds = allCinds
      .filter { cind =>
      ConditionCodes.isBinaryCondition(cind.depCaptureType) && ConditionCodes.isBinaryCondition(cind.refCaptureType)
    }.name("Filter 2/2 CINDs")


    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_VERBOSE) {
      singleSingleCinds.map(x => s"1/1 CIND: $x").output(new PrintingOutputFormat())
      doubleSingleCinds.map(x => s"2/1 CIND: $x").output(new PrintingOutputFormat())
      singleDoubleCinds.map(x => s"1/2 CIND: $x").output(new PrintingOutputFormat())
      doubleDoubleCinds.map(x => s"2/2 CIND: $x").output(new PrintingOutputFormat())
    }

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS) {
      singleSingleCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x 1/1 CINDs (non-minimal).").output(new PrintingOutputFormat())
      singleDoubleCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x 1/2 CINDs (non-minimal).").output(new PrintingOutputFormat())
      doubleSingleCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x 2/1 CINDs (non-minimal).").output(new PrintingOutputFormat())
      doubleDoubleCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x 2/2 CINDs (non-minimal).").output(new PrintingOutputFormat())
      allCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x non-minimal CINDs.").output(new PrintingOutputFormat())
    }

    // Remove redundant CINDs if requested.
    if (parameters.isCleanImplied) {
      allCinds = removeImpliedCinds(singleSingleCinds, singleDoubleCinds, doubleSingleCinds, doubleDoubleCinds, parameters)
      if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS) {
        allCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x minimal CINDs.").output(new PrintingOutputFormat())
      }
    }


    allCinds
  }

  /**
   * This helper method removes implied CINDs and return a [[DataSet]] that unifies them into a minimal CIND set.
   * However, this method only checks for direct implication, i.e., 2/1 CINDs that are implied by 1/2 CINDs w/o having
   * the corresponding 2/2 or 1/1 CINDs will not be minimized.
   */
  def removeImpliedCinds(unaryUnaryCinds: DataSet[Cind],
                         unaryBinaryCinds: DataSet[Cind],
                         binaryUnaryCinds: DataSet[Cind],
                         binaryBinaryCinds: DataSet[Cind],
                         parameters: Parameters) = {

    val minimalBinaryUnaryCinds = binaryUnaryCinds
      .coGroup(unaryUnaryCinds)
      .where("refCaptureType", "refConditionValue1")
      .equalTo("refCaptureType", "refConditionValue1")
      .apply(new RemoveNonMinimalDoubleXxxCinds)
      .name("Remove 2/1 CINDs implied by 1/1 CINDs")
      .coGroup(binaryBinaryCinds)
      .where("depCaptureType", "depConditionValue1", "depConditionValue2")
      .equalTo("depCaptureType", "depConditionValue1", "depConditionValue2")
      .apply(new RemoveNonMinimalXxxSingleCinds)
      .name("Remove 2/1 CINDs implied by 2/2 CINDs")

    val minimalUnaryUnaryCinds = unaryUnaryCinds
      .coGroup(unaryBinaryCinds)
      .where("depCaptureType", "depConditionValue1") // depConditionValue2 is known to be ""
      .equalTo("depCaptureType", "depConditionValue1") // depConditionValue2 is known to be ""
      .apply(new RemoveNonMinimalXxxSingleCinds)
      .name("Remove 1/1 CINDs implied by 1/2 CINDs")

    val minimalBinaryBinaryCinds = binaryBinaryCinds
      .coGroup(unaryBinaryCinds)
      .where("refCaptureType", "refConditionValue1", "refConditionValue2")
      .equalTo("refCaptureType", "refConditionValue1", "refConditionValue2")
      .apply(new RemoveNonMinimalDoubleXxxCinds)
      .name("Remove 2/2 CINDs implied by 1/2 CINDs")

    if (parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS) {
      minimalUnaryUnaryCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x minimal 1/1 CINDs.").output(new PrintingOutputFormat())
      minimalBinaryUnaryCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x minimal 2/1 CINDs.").output(new PrintingOutputFormat())
      binaryBinaryCinds.map(x => 1).reduce(_ + _).map(x => s"Found $x minimal 2/2 CINDs.").output(new PrintingOutputFormat())
    }

    minimalUnaryUnaryCinds
      .union(minimalBinaryUnaryCinds)
      .union(unaryBinaryCinds)
      .union(minimalBinaryBinaryCinds)
  }

  def extractJoinLineSize: (JoinLine) => Int

  def extractMaximumJoinLineSize: (JoinLineLoad) => Int

}

object TraversalStrategy {

  // TODO: Figure out what the result will actually be. Maybe, it is only a single data set.
  case class Result(x: Any)

}