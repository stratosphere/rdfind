package de.hpi.isg.sodap.rdfind.programs

import java.io.{BufferedReader, File, IOException, InputStreamReader}
import java.net.URI
import java.util.zip.GZIPInputStream

import com.google.common.hash.BloomFilter
import com.google.common.io.CountingInputStream
import de.hpi.isg.rdf_converter.parser.{NQuadsParser, NTriplesParser}
import de.hpi.isg.sodap.flink.jobs.AbstractFlinkProgram
import de.hpi.isg.sodap.flink.persistence.MultiFileTextInputFormat
import de.hpi.isg.sodap.flink.util.RemoteCollectorUtils
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators._
import de.hpi.isg.sodap.rdfind.plan._
import de.hpi.isg.sodap.rdfind.programs.RDFind._
import de.hpi.isg.sodap.rdfind.util.{ConditionCodes, UntypedBloomFilterParameters}
import de.hpi.isg.sodap.util.configuration.StratosphereParameters
import org.apache.flink.api.java.io.{DiscardingOutputFormat, PrintingOutputFormat, RemoteCollectorConsumer, RemoteCollectorImpl}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.{DataSet => ScalaDataSet, _}
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * This job simply counts the number of conditions in an RDF dataset.
 *
 * @author sebastian.kruse 
 * @since 01.04.2015
 */
class CountConditions(args: String*) extends
AbstractFlinkProgram[Parameters](args: _*) {

  lazy val logger = LoggerFactory.getLogger(getClass)

  /** Holds CLI parameters. */
  private var parameters: Parameters = _

  private val baseBloomFilterParameters = UntypedBloomFilterParameters(10000000, 0.01)

  private lazy val traversalStrategy: TraversalStrategy = this.parameters.traversalStrategy match {
    case 0 => new AllAtOnceTraversalStrategy
    case 1 => new SmallToLargeTraversalStrategy(baseBloomFilterParameters)
    case 2 => new ApproximateAllAtOnceTraversalStrategy
    case 3 => new LateBBTraversalStrategy
    case _ => throw new scala.IllegalArgumentException(s"Unknown traversal strategy: ${this.parameters.traversalStrategy}")
  }

  override protected def getStratosphereParameters: StratosphereParameters = this.parameters.stratosphereParameters

  override protected def initialize(args: String*): Unit = {
    this.parameters = parseCommandLine(args.toArray)
  }

  override protected def createParameters(): Parameters = new Parameters

  override protected def executeProgramLogic(): Unit = {
    // Assemble the Flink plan.
    createFlinkPlan()

        // Execute the Flink plan.
    val inputFileNames = this.parameters.inputFilePaths.map(path => new Path(path).getName)
    executePlan(s"Count attributes in ${inputFileNames.mkString(":")}")
  }


  override protected def onExit(): Unit = {
    super.onExit()
    RemoteCollectorImpl.shutdownAll()
  }

  /**
   * Collect the input paths from the parameters and expand path patterns.
   */
  private def resolvePathPatterns(rawPaths: String*) = {
    rawPaths.flatMap(path => resolvePathPattern(path)).toList
  }

  /**
   * Expands a single path pattern.
   */
  private def resolvePathPattern(rawInputPath: String): Traversable[String] = {
    try {
      if (rawInputPath.contains("*")) {
        // If the last path of the pattern contains an asterisk, expand the path.
        // Check that the asterisk is contained in the last path segment.
        val lastSlashPos = rawInputPath.lastIndexOf('/');
        if (rawInputPath.indexOf('*') < lastSlashPos) {
          throw new RuntimeException("Path expansion is only possible on the last path segment: " + rawInputPath);
        }

        // Collect all children of the to-be-expanded path.
        val lastSegmentRegex = rawInputPath.substring(lastSlashPos + 1)
          .replace(".", "\\.")
          .replace("[", "\\[")
          .replace("]", "\\]")
          .replace("(", "\\(")
          .replace(")", "\\)")
          .replace("*", ".*")
        val parentPath = new Path(rawInputPath.substring(0, lastSlashPos));
        val fs = parentPath.getFileSystem();

        fs.listStatus(parentPath)
          .filter(_.getPath.getName.matches(lastSegmentRegex))
          .map(_.getPath.toString)
      } else {
        List(rawInputPath)
      }
    } catch {
      case e: IOException => throw new RuntimeException("Could not expand paths.", e);
    }
  }

  /**
   * Creates a Flink plan for finding CINDs.
   */
  def createFlinkPlan(): Unit = {

    val inputFilePaths = resolvePathPatterns(this.parameters.inputFilePaths: _*)

    // Read the triples.
    val inputFormat = MultiFileTextInputFormat.createFor(inputFilePaths: _*)
    val lines = new ScalaDataSet(this.executionEnvironment.createInput(inputFormat))
      .name("Read input files")

    if (inputFilePaths(0).startsWith("file:") && getStratosphereParameters.degreeOfParallelism > 0) {
      lines.setParallelism(1)
    }
    val filterLines = lines
      .filter(!_.f1.startsWith("#"))
      .name("Filter comments")

    if (getStratosphereParameters.degreeOfParallelism > 0) {
      filterLines.setParallelism(this.executionEnvironment.getParallelism)
    }

    // A pragmatic way to allow nq files. All files must be of the same type though.
    var triples =
      if (inputFilePaths.get(0).endsWith("nq")) {
        filterLines.map { in: Tuple2[Integer, String] =>
          val fields = new NQuadsParser().parse(in.f1)
          RDFTriple(fields(0), fields(1), fields(2))
        }
      } else {
        val isTabInputFile = this.parameters.isInputFileWithTabs
        filterLines
          .mapPartition { (in: Iterator[Tuple2[Integer, String]], out: Collector[RDFTriple]) =>
          val parser = if (isTabInputFile) new NTriplesParser('\t') else new NTriplesParser()
          in.foreach { line =>
            val fields = parser.parse(line.f1)
            out.collect(RDFTriple(fields(0), fields(1), fields(2)))
          }
        }
      }
    triples.name("Parse triples")

    if (this.parameters.prefixFilePaths != null && this.parameters.prefixFilePaths.nonEmpty) {

      val prefixFilePaths = resolvePathPatterns(this.parameters.prefixFilePaths: _*)

      // Read the prefix files.
      val inputFormat = MultiFileTextInputFormat.createFor(prefixFilePaths: _*)
      val lines = new ScalaDataSet(this.executionEnvironment.createInput(inputFormat))
        .name("Read prefix files")
      if (prefixFilePaths(0).startsWith("file:") && getStratosphereParameters.degreeOfParallelism > 0) {
        lines.setParallelism(1)
      }

      // Parse the prefix lines.
      val prefixes = lines
        .filter(!_.f1.startsWith("#"))
        .name("Filter comments")
        .map(new ParseRdfPrefixes)
        .name("Parse RDF prefixes")

      // Shorten the triples using the prefixes.
      triples = triples
        .map(new ShortenUrls)
        .withBroadcastSet(prefixes, ShortenUrls.PREFIX_BROADCAST)
        .name("Shorten URLs")
    }


    // Check if it is requested to create distinct triples at first.
    if (this.parameters.isEnsureDistinctTriples) {
      triples = triples.distinct.name("Remove duplicate triples")
    }

    val countsByType = triples.flatMap{ (t: RDFTriple, out: Collector[ConditionCount]) =>
      out.collect(ConditionCount(ConditionCodes.subjectCondition, t.subj, "", 1))
      out.collect(ConditionCount(ConditionCodes.predicateCondition, t.pred, "", 1))
      out.collect(ConditionCount(ConditionCodes.objectCondition, t.obj, "", 1))
      out.collect(ConditionCount(ConditionCodes.subjectPredicateCondition, t.subj, t.pred, 1))
      out.collect(ConditionCount(ConditionCodes.subjectObjectCondition, t.subj, t.obj, 1))
      out.collect(ConditionCount(ConditionCodes.predicateObjectCondition, t.pred, t.obj, 1))
    }
    .groupBy("captureType", "conditionValue1", "conditionValue2")
    .reduce { (a: ConditionCount, b: ConditionCount) =>
      a.count += b.count
      a
    }
//      .print()
      .map(count => (count.captureType, count.count))

    val histogramByType = countsByType.map(c => (c._1, c._2, 1)).groupBy(0, 1).sum(2)

    val overallHistogram = countsByType.map(c => (0, c._2, 1)).groupBy(0, 1).sum(2)

    val allHistograms = histogramByType.union(overallHistogram).map(x => x)
    allHistograms.map(x => s"${x._1};${x._2};${x._3}").print()
    new ScalaDataSet[Int](this.executionEnvironment.fromCollection(0 to 15000)).map((x: Int) => (x, 0))
  }

}

object CountConditions {
  def main(args: Array[String]) {
    new CountConditions(args: _*).run()
  }
}


