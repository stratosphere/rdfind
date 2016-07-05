package de.hpi.isg.sodap.rdfind.programs

import java.util

import com.beust.jcommander.{Parameter, ParametersDelegate}
import de.hpi.isg.rdf_converter.parser.NTriplesParser
import de.hpi.isg.sodap.flink.jobs.AbstractFlinkProgram
import de.hpi.isg.sodap.flink.persistence.MultiFileTextInputFormat
import de.hpi.isg.sodap.flink.util.RemoteCollectorUtils
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.{CreateJoinPartners, UnionConditions}
import de.hpi.isg.sodap.rdfind.plan.{AllAtOnceTraversalStrategy, FrequentConditionPlanner, SmallToLargeTraversalStrategy}
import de.hpi.isg.sodap.rdfind.programs.CheckHashCollisions._
import de.hpi.isg.sodap.util.configuration.StratosphereParameters
import org.apache.flink.api.java.io.{PrintingOutputFormat, RemoteCollectorConsumer}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.{DataSet => ScalaDataSet, _}
import org.apache.flink.core.fs.FileSystem
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

/**
 * This is for playing around with RDF CINDs.
 *
 * @author sebastian.kruse 
 * @since 01.04.2015
 */
class CheckHashCollisions(args: String*) extends
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

    // Read the triples.
    val inputFormat = MultiFileTextInputFormat.createFor(this.parameters.inputFilePaths: _*)
    val lines = new ScalaDataSet(this.executionEnvironment.createInput(inputFormat))
    val triples = lines
      .filter(!_.f1.startsWith("#"))
      .map { in: Tuple2[Integer, String] =>
      val fields = new NTriplesParser().parse(in.f1)
      RDFTriple(fields(0), fields(1), fields(2))
    }

    val allValues = triples.flatMap(triple => List(triple.subj, triple.pred, triple.obj))

    val hashedValues = allValues.map(value => (value, MurmurHash3.stringHash(value) & 0x7FFF7FFF))
//    val hashedValues = allValues.map(value => (value, value.hashCode & 0xFFFFFFFF))

    val collisionCount = hashedValues.distinct.map(x => (x._2, 1)).groupBy(0).sum(1)

    collisionCount.map(x => if (x._2 > 1) 0 else 1).reduce(_ + _).map(x => s"There are $x collision-free hashes.").output(new PrintingOutputFormat())
    collisionCount.map(x => if (x._2 > 1) x._2 else 0).reduce(_ + _).map(x => s"There are $x hashes with collisions.").output(new PrintingOutputFormat())


    executePlan(s"Finding hash-collisions in ${this.parameters.inputFilePaths}")

  }


}

object CheckHashCollisions {
  private[rdfind] class Parameters {
    @ParametersDelegate
    val stratosphereParameters = new StratosphereParameters
    @Parameter(description = "input files to process")
    var inputFilePaths: java.util.List[String] = new util.ArrayList[String]
  }

  def main(args: Array[String]) {
    new CheckHashCollisions(args: _*).run()
  }
}


