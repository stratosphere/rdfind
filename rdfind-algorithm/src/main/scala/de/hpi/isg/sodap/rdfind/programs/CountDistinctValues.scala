package de.hpi.isg.sodap.rdfind.programs

import java.io.IOException
import java.util

import com.beust.jcommander.{Parameter, ParametersDelegate}
import de.hpi.isg.rdf_converter.parser.{NQuadsParser, NTriplesParser}
import de.hpi.isg.sodap.flink.jobs.AbstractFlinkProgram
import de.hpi.isg.sodap.flink.persistence.MultiFileTextInputFormat
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators._
import de.hpi.isg.sodap.rdfind.programs.CountDistinctValues.Parameters
import de.hpi.isg.sodap.util.configuration.StratosphereParameters
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.{DataSet => ScalaDataSet, _}
import org.apache.flink.core.fs.Path
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * This is for playing around with RDF CINDs.
 *
 * @author sebastian.kruse 
 * @since 01.04.2015
 */
class CountDistinctValues(args: String*) extends
AbstractFlinkProgram[Parameters](args: _*) {

  lazy val logger = LoggerFactory.getLogger(getClass)

  /** Holds CLI parameters. */
  private var parameters: Parameters = _

  override protected def getStratosphereParameters: StratosphereParameters = this.parameters.stratosphereParameters

  override protected def initialize(args: String*): Unit = {
    this.parameters = parseCommandLine(args.toArray)
  }

  override protected def createParameters(): Parameters = new Parameters

  override protected def executeProgramLogic(): Unit = {
    logger.debug("Input files: {}", this.parameters.inputFilePaths)

    val inputFilePaths = resolvePathPatterns(this.parameters.inputFilePaths: _*)

    // Read the triples.
    val inputFormat = MultiFileTextInputFormat.createFor(inputFilePaths: _*)
    val lines = new ScalaDataSet(this.executionEnvironment.createInput(inputFormat))
      .name("Read input files")

    if (inputFilePaths.head.startsWith("file:") && getStratosphereParameters.degreeOfParallelism > 0) {
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
        val isTabInputFile = this.parameters.isTabSeparatedInputFiles
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
      if (prefixFilePaths.head.startsWith("file:") && getStratosphereParameters.degreeOfParallelism > 0) {
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


    val allValues = triples.flatMap(triple => List(triple.subj, triple.pred, triple.obj))
    val localCounters = allValues
      .map(scala.Tuple1(_)).distinct.map(_._1)
      .map(value => if (value != null && value.startsWith("\"")) (0, 1) else (1, 0))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      .collect()

    localCounters.foreach(counter => println(s"Counted ${counter._1} URLs and ${counter._2} literals."))
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
        val lastSlashPos = rawInputPath.lastIndexOf('/')
        if (rawInputPath.indexOf('*') < lastSlashPos) {
          throw new RuntimeException("Path expansion is only possible on the last path segment: " + rawInputPath)
        }

        // Collect all children of the to-be-expanded path.
        val lastSegmentRegex = rawInputPath.substring(lastSlashPos + 1)
          .replace(".", "\\.")
          .replace("[", "\\[")
          .replace("]", "\\]")
          .replace("(", "\\(")
          .replace(")", "\\)")
          .replace("*", ".*")
        val parentPath = new Path(rawInputPath.substring(0, lastSlashPos))
        val fs = parentPath.getFileSystem()

        fs.listStatus(parentPath)
          .filter(_.getPath.getName.matches(lastSegmentRegex))
          .map(_.getPath.toString)
      } else {
        List(rawInputPath)
      }
    } catch {
      case e: IOException => throw new RuntimeException("Could not expand paths.", e)
    }
  }


}

object CountDistinctValues {

  def main(args: Array[String]) {
    new CountDistinctValues(args: _*).run()
  }

  private[rdfind] class Parameters {
    @ParametersDelegate
    val stratosphereParameters = new StratosphereParameters
    @Parameter(description = "input files to process")
    var inputFilePaths: java.util.List[String] = new util.ArrayList[String]
    @Parameter(names = Array("--prefixes"), description = "a list of nt-prefix files to apply on the input triple")
    var prefixFilePaths: java.util.List[String] = new util.ArrayList[String]
    @Parameter(names = Array("--tabs"), description = "indicates that the input nt/nq files are tab-separated")
    var isTabSeparatedInputFiles = false

  }

}
