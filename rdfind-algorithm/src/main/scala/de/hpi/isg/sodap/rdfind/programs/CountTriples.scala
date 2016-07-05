package de.hpi.isg.sodap.rdfind.programs

import java.io.IOException
import java.util

import com.beust.jcommander.{Parameter, ParametersDelegate}
import de.hpi.isg.rdf_converter.parser.{NTriplesParser, NQuadsParser}
import de.hpi.isg.sodap.flink.jobs.AbstractFlinkProgram
import de.hpi.isg.sodap.flink.persistence.MultiFileTextInputFormat
import de.hpi.isg.sodap.rdfind.data.RDFTriple
import de.hpi.isg.sodap.rdfind.operators.{ShortenUrls, ParseRdfPrefixes}
import de.hpi.isg.sodap.rdfind.programs.CountTriples.Parameters
import de.hpi.isg.sodap.util.configuration.StratosphereParameters
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.{DataSet => ScalaDataSet}
import org.apache.flink.core.fs.Path
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import org.apache.flink.api.scala._


/**
 * This is for playing around with RDF CINDs.
 *
 * @author sebastian.kruse
 * @since 19.10.2015
 */
class CountTriples(args: String*) extends
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

    val numLines = filterLines.count()

    println(s"Counted $numLines.")
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


object CountTriples {

  def main(args: Array[String]) {
    new CountTriples(args: _*).run()
  }

  private[rdfind] class Parameters {
    @ParametersDelegate
    val stratosphereParameters = new StratosphereParameters
    @Parameter(description = "input files to process")
    var inputFilePaths: java.util.List[String] = new util.ArrayList[String]
    @Parameter(names = Array("--tabs"), description = "indicates that the input nt/nq files are tab-separated")
    var isTabSeparatedInputFiles = false

  }

}
