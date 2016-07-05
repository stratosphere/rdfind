package de.hpi.isg.sodap.rdfind.programs

import java.io.{BufferedReader, File, IOException, InputStreamReader}
import java.net.URI
import java.security.MessageDigest
import java.util
import java.util.zip.GZIPInputStream

import com.beust.jcommander.{Parameter, ParametersDelegate}
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
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import de.hpi.isg.sodap.rdfind.util.{HashFunction, UntypedBloomFilterParameters}
import de.hpi.isg.sodap.util.configuration.StratosphereParameters
import org.apache.flink.api.java.io.{DiscardingOutputFormat, PrintingOutputFormat, RemoteCollectorConsumer, RemoteCollectorImpl}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.{DataSet => ScalaDataSet, _}
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
 * This is for playing around with RDF CINDs.
 *
 * @author sebastian.kruse 
 * @since 01.04.2015
 */
class RDFind(args: String*) extends
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

  private var frequentCaptureBloomFilter: ScalaDataSet[BloomFilter[Condition]] = _

  override protected def getStratosphereParameters: StratosphereParameters = this.parameters.stratosphereParameters

  override protected def initialize(args: String*): Unit = {
    this.parameters = parseCommandLine(args.toArray)
  }

  override protected def createParameters(): Parameters = new Parameters

  override protected def executeProgramLogic(): Unit = {
    logger.debug("Input files: {}", this.parameters.inputFilePaths)

    // Assemble the Flink plan.
    createFlinkPlan()

    // Output the Flink plan if desired.
    if (this.parameters.isPrintExecutionPlan) {
      println(
        s""""=== Execution plan ================================================================
           |${this.executionEnvironment.getExecutionPlan}
            |==================================================================================="""
          .stripMargin)
    }

    // Execute the Flink plan.
    val inputFileNames = this.parameters.inputFilePaths.map(path => new Path(path).getName)
    executePlan(s"RDFind on ${inputFileNames.mkString(":")} [TS: ${this.parameters.traversalStrategy}, " +
      s"${this.parameters.projectionAttributes}, " +
      s">=${this.parameters.minSupport}]")
  }


  override protected def onExit(): Unit = {
    super.onExit()
    RemoteCollectorImpl.shutdownAll()
  }

  /**
   * Estimates the number of triples contained in the given files.
   * @param paths are paths to the files whose size is to be estimated
   * @return the size estimate
   */
  def estimateNumberOfTriples(paths: String*): Long =
    paths.map(path => estimateNumberOfTriples(new Path(path))).sum

  /**
   * Estimates the number of triples contained in the given file.
   * @param path describes the file whose size is to be estimated
   * @return the size estimate
   */
  def estimateNumberOfTriples(path: Path): Long = {
    val fs = path.getFileSystem
    var numLines = 0L
    var numReadBytes = 0L
    var reader: BufferedReader = null
    try {
      val (encodedInputStream, countingInputStream) = open(path, fs)
      reader = new BufferedReader(new InputStreamReader(encodedInputStream, "UTF-8"), 1)
      var line = reader.readLine()
      while (line != null && numLines < 10000) {
        if (!line.startsWith("#")) numLines += 1
        numReadBytes += line.getBytes("UTF-8").length + 1
        line = reader.readLine()
      }
    } catch {
      case e: IOException => throw new RuntimeException(s"Could not estimate lines of $path.", e)
    } finally {
      if (reader != null) reader.close()
    }

    val estimate =
      if (numLines == 0) 0
      else (fs.getFileStatus(path).getLen.asInstanceOf[Double] / numReadBytes * numLines).asInstanceOf[Long];

    getLogger.info(s"Estimating $estimate triples to be in $path")

    estimate
  }

  /**
   * Opens the given input path. If it has a known compression (i.e., .gz), it will be decompressed.
   */
  private def open(path: Path, fs: FileSystem): (java.io.InputStream, CountingInputStream) = {
    val fos = new CountingInputStream(fs.open(path))
    val lastDotPos = path.getName().lastIndexOf(".");
    val extension = if (lastDotPos >= 0) path.getName().substring(lastDotPos + 1) else null
    (extension match {
      case "gz" => new GZIPInputStream(fos)
      case _ => fos
    }, fos)
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

    // Estimate number of triples.
    val estTriples = estimateNumberOfTriples(inputFilePaths: _*)

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

    if (this.parameters.isAsciifyTriples) {
      triples = triples.map(new AsciifyTriples)
    }

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

    if (this.parameters.isOnlyRead) {
      triples.output(new DiscardingOutputFormat[RDFTriple])
      return
    }

    if (this.parameters.isApplyHash) {
      triples = triples.map { triple =>
        triple.subj = RDFind.hash(triple.subj)
        triple.pred = RDFind.hash(triple.pred)
        triple.obj = RDFind.hash(triple.obj)

        triple
      }.name("Hash triple values")
    }

    // Check if it is requested to create distinct triples at first.
    if (this.parameters.isEnsureDistinctTriples) {
      triples = triples.distinct.name("Remove duplicate triples")
    }

    // Find frequent conditions.
    val frequentConditionDataSets =
      if (this.parameters.isUseFrequentItemSet) {
        val frequentConditionPlanner = new FrequentConditionPlanner(this.parameters, baseBloomFilterParameters.copy())
        frequentConditionPlanner.constructFrequentConditionPlan(triples, estTriples)
      } else {
        FrequentConditionPlanner.noDataSets
      }

    if (this.parameters.findOnlyFrequentConditions >= 2) {
      frequentConditionDataSets.frequentDoubleConditionBloomFilter
        .output(new DiscardingOutputFormat[(Int, BloomFilter[Condition])])
    }
    if (this.parameters.findOnlyFrequentConditions >= 1) {
      frequentConditionDataSets.frequentSingleConditionBloomFilter
        .output(new DiscardingOutputFormat[(Int, BloomFilter[String])])
      return
    }

    // Prepare the join of the triples.
    val isAlreadyCheckBinaryFrequentConditions = this.parameters.isUseFrequentItemSet && !this.parameters.isCreateAnyBinaryCaptures
    val joinPartners = triples.flatMap(new CreateJoinPartners(this.parameters.isUseFrequentItemSet,
      isAlreadyCheckBinaryFrequentConditions, this.parameters.isUseAssociationRules, this.parameters.projectionAttributes,
      this.parameters.isHashBasedDictionaryCompression, this.parameters.hashAlgorithm, this.parameters.hashBytes))
      .name("Create joinable capture instances")
    if (this.parameters.isUseFrequentItemSet) {
      joinPartners.withBroadcastSet(frequentConditionDataSets.frequentSingleConditionBloomFilter,
        CreateJoinPartners.UNARY_FC_BLOOM_FILTERS_BROADCAST)
    }
    if (this.parameters.isUseAssociationRules) {
      joinPartners.withBroadcastSet(frequentConditionDataSets.associationRules,
        CreateJoinPartners.ASSOCIATION_RULE_BROADCAST)
    }
    if (isAlreadyCheckBinaryFrequentConditions) {
      joinPartners.withBroadcastSet(frequentConditionDataSets.frequentDoubleConditionBloomFilter,
        CreateJoinPartners.BINARY_FC_BLOOM_FILTERS_BROADCAST)
    }
    if (this.parameters.isHashBasedDictionaryCompression) {
      joinPartners.withBroadcastSet(frequentConditionDataSets.collisionHashes, CreateJoinPartners.COLLISION_HASHES_BROADCAST)
    }

    // Create the co-occurrence list for conditions and, if load-balancing is requested, gather statistics.
    var averageJoinLineLoad: ScalaDataSet[JoinLineLoad] = null
    val predicateJoin = {
      var tempJoin =
        if (this.parameters.isNotCombinableJoin) {
          joinPartners
            .groupBy("joinValue")
            .reduceGroup(new UnionConditions())
        } else {
          joinPartners
            .groupBy("joinValue")
            .combineGroup(new UnionJoinCandidates())
            .name("Combine joinable capture instances")
            .groupBy("joinKey")
            .reduceGroup(new UnionCombinedJoinCandidates())
            .name("Join combined capture instances")
        }

      if (this.parameters.isFindFrequentCaptures) {

        // Determine the support of the different captures.
        val minSupport = this.parameters.minSupport
        var frequentCaptureCounts = tempJoin
          .flatMap { (joinLine: JoinLine, out: Collector[ConditionCount]) =>
          val conditions = new mutable.HashSet[Condition]()
          joinLine.conditions.foreach { condition =>
            condition.coalesce()
            conditions += condition
            if (!condition.isUnary) {
              val conditionCodes = decodeConditionCode(condition.conditionType, isRequireDoubleCode = true)
              val newConditionCode1 = createConditionCode(conditionCodes._1, secondaryCondition = conditionCodes._3)
              conditions += Condition(condition.conditionValue1, "", newConditionCode1)
              val newConditionCode2 = createConditionCode(conditionCodes._2, secondaryCondition = conditionCodes._3)
              conditions += Condition(condition.conditionValue2, "", newConditionCode2)
            }
          }
          conditions.foreach(x => out.collect(x.toConditionCount(count = 1)))
        }
          .groupBy("captureType", "conditionValue1", "conditionValue2").sum("count")
          .filter(_.count >= minSupport)
        if (parameters.counterLevel >= 2) {
          frequentCaptureCounts = frequentCaptureCounts.filter(new CountItems[ConditionCount]("frequent-capture-counter"))

        }
        val frequentCaptures = frequentCaptureCounts.map(_.toCondition)

        val conditionBloomFilterParameters = this.baseBloomFilterParameters.withType[Condition](classOf[Condition.Funnel].getName)
        this.frequentCaptureBloomFilter = frequentCaptures
          .mapPartition {
          conditions: Iterator[Condition] =>
            val bloomFilter = conditionBloomFilterParameters.createBloomFilter
            var count = 0
            while (conditions.hasNext) {
              val condition = conditions.next()
              bloomFilter.put(condition)
              count += 1
            }

            println(s"Added $count infrequent captures to the partial Bloom filter.")
            List(bloomFilter)
        }.name("Create partial infrequent capture Bloom filters")
          .reduceGroup {
          (iterator: Iterator[BloomFilter[Condition]], out: Collector[BloomFilter[Condition]]) =>
            val bloomFilter = iterator.next()
            while (iterator.hasNext) {
              val nextBloomFilter = iterator.next()
              bloomFilter.putAll(nextBloomFilter)
            }
            out.collect(bloomFilter)
        }.name("Merge partial infrequent capture Bloom filters")
      }


      // Rebalance the join if requested.
      if (this.parameters.isRebalanceJoin) {
        val parallelism = getStratosphereParameters.degreeOfParallelism
        if (parallelism == -1) {
          throw new IllegalStateException("Must set explicit parallelism to make use of load balancing!")
        }

        // Determine the size of the join lines.
        tempJoin = tempJoin.map(new AnnotateJoinLineSizes).name("Annotate join line sizes")

        //        val averageLoad = tempJoin
        //          .map { joinLine => val size = joinLine.conditions.length.asInstanceOf[Long]; size * size }
        //          .reduce(_ + _)
        //          .map(numCapturePairs => Math.round(Math.sqrt(numCapturePairs / parallelism.asInstanceOf[Double])).asInstanceOf[Int])
        //          .map(x => s"Average load join line size is $x.")
        //          .printOnTaskManager("DEBUG")

        // Determine the average load for each task slot caused by the join lines.
        averageJoinLineLoad = tempJoin
          .map(new DetermineJoinLineLoads).name("Determine join line loads")
          .reduce(_ += _).name("Sum up join line loads")
          .map(_ /= parallelism).name("Calculate average join line load")

        // Split large elements.
        tempJoin = tempJoin
          .flatMap(new AssignJoinLineRebalancing(parallelism,
          this.parameters.rebalanceFactor,
          this.parameters.rebalanceMaxLoad,
          this.traversalStrategy.extractMaximumJoinLineSize,
          this.traversalStrategy.extractJoinLineSize))
          .name("Assign join line rebalancing")
          .withBroadcastSet(averageJoinLineLoad, AssignJoinLineRebalancing.AVERAGE_JOIN_LINE_LOAD_BROADCAST)

        // Repartition the elements.
        tempJoin = this.parameters.rebalanceStrategy match {
          case 1 => tempJoin.partitionCustom[Int](new JoinLineRebalancePartitioner, "partitionKey")
          case 2 => tempJoin.partitionCustom[Int](new LoadBasedPartitioner, (joinLine: JoinLine) => joinLine.numCombinedConditions)
          case _ => throw new IllegalArgumentException(s"Illegal rebalancing strategy.")
        }

        //        averageJoinLineLoad.map(x => s"Detaild average load join line load is $x.").printOnTaskManager("DEBUG")
      }

      tempJoin
    }

    if (this.parameters.isCreateJoinHistogram) {
      val histogram = predicateJoin.map(x => (x.conditions.length, 1)).groupBy(0).sum(1).collect()
      histogram.toArray.sortBy(_._1).foreach(entry => println(s"Join size ${entry._1} encountered ${entry._2}x"))
    }

    if (this.parameters.isOnlyJoin) {
      predicateJoin.output(new DiscardingOutputFormat[JoinLine])
      return
    }

    var result = this.traversalStrategy.enhanceFlinkPlan(predicateJoin, frequentConditionDataSets, this.frequentCaptureBloomFilter, this.parameters)

    if (frequentConditionDataSets.dictionary != null) {
      // IDEA: Null values are very common for the second condition values. That leads to a skew in the decompression.
      // Hence, we could generate arbitrary null values, so as to evenly distribute these.
      // Antoher possibility is to use a custom partitioner that maps null-values in a round-robin fashion.
      result = result
        .coGroup(frequentConditionDataSets.dictionary)
        .where("depConditionValue1")
        .equalTo(0)
        .apply(new ConditionDecompressor[Cind](_.depConditionValue1, _.depConditionValue1 = _))
        .name("Decompressing dependent conditions' first values")
        .coGroup(frequentConditionDataSets.dictionary)
        .where("refConditionValue1")
        .equalTo(0)
        .apply(new ConditionDecompressor[Cind](_.refConditionValue1, _.refConditionValue1 = _))
        .name("Decompressing referenced conditions' first values")
        .coGroup(frequentConditionDataSets.dictionary)
        .where("depConditionValue2")
        .equalTo(0)
        .withPartitioner(new DecompressionPartitioner)
        .apply(new ConditionDecompressor[Cind](_.depConditionValue2, _.depConditionValue2 = _))
        .name("Decompressing dependent conditions' second values")
        .coGroup(frequentConditionDataSets.dictionary)
        .where("refConditionValue2")
        .equalTo(0)
        .withPartitioner(new DecompressionPartitioner)
        .apply(new ConditionDecompressor[Cind](_.refConditionValue2, _.refConditionValue2 = _))
        .name("Decompressing referenced conditions' second values")
    }

    if (parameters.counterLevel >= 1) {
      result = result.map(new CountItemsUsingMap[Cind](RDFind.CIND_COUNT_ACCUMULATOR))
    }

    if (this.parameters.debugLevel >= RDFind.DEBUG_LEVEL_STATISTICS)
      result.map(x => 1).reduce(_ + _).map(x => s"Found $x CINDs in total.").output(new PrintingOutputFormat())

    if (this.parameters.debugLevel >= RDFind.DEBUG_LEVEL_SANITY) {
      val trivialCinds = result.filter { cind =>
        val depCondition = Condition(cind.depConditionValue1, cind.depConditionValue2, cind.depCaptureType)
        val refCondition = Condition(cind.refConditionValue1, cind.refConditionValue2, cind.refCaptureType)
        refCondition.isImpliedBy(depCondition)
      }
      trivialCinds.map(x => 1).reduce(_ + _).map(x => s"$x of the CINDs are trivial.").output(new PrintingOutputFormat())
    }

    var isResultConsumed = false
    if (this.parameters.outputFile != null && !this.parameters.outputFile.isEmpty) {
      val output = result
        .map(_.toString)
        .name("Print CINDs")
        .writeAsText(this.parameters.outputFile, FileSystem.WriteMode.OVERWRITE)
        .name(s"Write CINDs to ${this.parameters.outputFile}")
      val outputURI = new URI(this.parameters.outputFile)
      if (outputURI.getScheme == "file") {
        // Little hack for local execution: Create only a single file and make sure its folder exists already.
        output.setParallelism(1)
        val outputFile = new File(outputURI)
        println(s"Outputting CINDs to ${outputFile.getAbsolutePath}.")
        outputFile.getParentFile.mkdirs()
      }
      isResultConsumed = true
    }

    if (this.parameters.assocationRuleOutputFile != null && !this.parameters.assocationRuleOutputFile.isEmpty) {
      val associationRules =
        if (this.parameters.isHashBasedDictionaryCompression) {
          frequentConditionDataSets.associationRules
            .coGroup(frequentConditionDataSets.dictionary)
            .where("antecedent")
            .equalTo(0)
            .apply(new ConditionDecompressor[AssociationRule](_.antecedent, _.antecedent = _))
            .name("Decompressing association rule antecedents")
            .coGroup(frequentConditionDataSets.dictionary)
            .where("consequent")
            .equalTo(0)
            .apply(new ConditionDecompressor[AssociationRule](_.consequent, _.consequent = _))
            .name("Decompressing association rule consequents")
        } else frequentConditionDataSets.associationRules

      val output = associationRules
        .map(_.toString)
        .name("Print assocation rules")
        .writeAsText(this.parameters.assocationRuleOutputFile, FileSystem.WriteMode.OVERWRITE)
        .name(s"Write assocation rules to ${this.parameters.assocationRuleOutputFile}")
      val outputURI = new URI(this.parameters.assocationRuleOutputFile)
      if (outputURI.getScheme == "file") {
        // Little hack for local execution: Create only a single file and make sure its folder exists already.
        output.setParallelism(1)
        val outputFile = new File(outputURI)
        println(s"Outputting assocation rules to ${outputFile.getAbsolutePath}.")
        outputFile.getParentFile.mkdirs()
      }
      isResultConsumed = true
    }

    if (this.parameters.isCollectResult) {
      val remoteCollectorOutputFormat = RemoteCollectorUtils.create(new RemoteCollectorConsumer[Cind] {

        override def collect(cind: Cind): Unit = {
          receiveCind(cind)
        }

      })
      result.output(remoteCollectorOutputFormat).name("Send CINDs to driver")
      isResultConsumed = true
    }

    if (this.parameters.debugLevel >= DEBUG_LEVEL_VERBOSE) {
      result.map(x => s"Final CIND: $x").output(new PrintingOutputFormat())
      isResultConsumed = true
    }

    if (!isResultConsumed) {
      result
        .map(x => 1).name("Create CIND counts")
        .reduce(_ + _).name("Add CIND counts")
        .map { x => println(s"Detected $x CINDs."); x }.name("Print CIND count")
        .output(new DiscardingOutputFormat).name("Discard CINDs")
    }
  }

  /**
   * This function is called when receiving a CIND from the task managers.
   */
  def receiveCind(cind: Cind): Unit = {
        println(cind)
  }

  override protected def cleanUp(): Unit = {
    super.cleanUp()
    RemoteCollectorImpl.shutdownAll()
  }
}

object RDFind {

  val CIND_COUNT_ACCUMULATOR: String = "cind-counter"

  val BROAD_CIND_COUNT_ACCUMULATOR: String = "broad-cind-counter"

  lazy val MESSAGE_DIGEST = new ThreadLocal[MessageDigest]()

  @deprecated
  def hashMd5(string: String): String = {
    var messageDigest = MESSAGE_DIGEST.get()
    if (messageDigest == null) {
      messageDigest = MessageDigest.getInstance("MD5")
      MESSAGE_DIGEST.set(messageDigest)
    }
    var digest: Array[Byte] = null
    messageDigest.reset()
    digest = messageDigest.digest(string.getBytes)
    val chars = new Array[Char]((digest.length + 1) >> 1)
    for (i <- 0 until digest.length) {
      val b = (0xFF & digest(i)).asInstanceOf[Char]
      if ((i & 0x1) == 0) {
        chars(i >> 1) = b
      } else {
        chars(i >> 1) = (chars(i >> i) | (b << 8)).asInstanceOf[Char]
      }
    }
    new String(chars)
  }

  @deprecated
  def hash(string: String): String = {
    val hash = MurmurHash3.stringHash(string) & 0x7FFF7FFF
    val chars = Array[Char]((hash >> 8).asInstanceOf[Char], (hash & 0xFFFF).asInstanceOf[Char])
    return new String(chars)
  }

  @deprecated
  val singleFisBroadcast = "single-fis-bloomfilters"

  def main(args: Array[String]) {
    new RDFind(args: _*).run()
  }

  private[rdfind] class Parameters {
    @ParametersDelegate
    val stratosphereParameters = new StratosphereParameters
    @Parameter(description = "input files to process")
    var inputFilePaths: java.util.List[String] = new util.ArrayList[String]
    @Parameter(names = Array("--prefixes"), description = "a list of nt-prefix files to apply on the input triple")
    var prefixFilePaths: java.util.List[String] = new util.ArrayList[String]
    @Parameter(names = Array("--distinct-triples"), description = "whether to ensure that triples are distinct")
    var isEnsureDistinctTriples = false
    @Parameter(names = Array("--asciify-triples"), description = "whether to replace non-ASCII characters in the input data")
    var isAsciifyTriples = false
    @Parameter(names = Array("--support"), description = "minimum support for conditions involved in CINDs")
    var minSupport: Int = 10
    @Parameter(names = Array("--traversal-strategy"), description = "ID of CIND search space traversal strategy")
    var traversalStrategy = 1
    @Parameter(names = Array("--use-fis"), description = "whether to find and use frequent item sets")
    var isUseFrequentItemSet = false
    @Parameter(names = Array("--use-ars"), description = "whether to find and use association rules")
    var isUseAssociationRules = false
    @Parameter(names = Array("--collect-result"), description = "whether to collect the results locally")
    var isCollectResult = false
    @Parameter(names = Array("--output"), description = "an output file to save the CINDs to")
    var outputFile: String = null
    @Parameter(names = Array("--ar-output"), description = "an output file to save the association rules to")
    var assocationRuleOutputFile: String = null
    @Parameter(names = Array("--clean-implied"), description = "whether to remove implied CINDs")
    var isCleanImplied = false
    @Parameter(names = Array("--frequent-condition-strategy"), description = "how to find frequent conditions")
    var frequentConditionStrategy = 0
    @Parameter(names = Array("--no-combinable-join"), description = "whether to use old-style pair-wise join of captures")
    var isNotCombinableJoin = false
    @Parameter(names = Array("--no-bulk-merge"), description = "whether to use old-style pair-wise merge of CIND candidates")
    var isNotBulkMerge = false
    @Parameter(names = Array("--rebalance-join"), description = "whether to rebalance the capture groups")
    var isRebalanceJoin = false
    @Parameter(names = Array("--rebalance-strategy"), description = "how to do the rebalacing (1, 2)")
    var rebalanceStrategy = 1
    @Parameter(names = Array("--rebalance-split"), description = "how to split attribute sets (1, 2)")
    var rebalanceSplitStrategy = 1
    @Parameter(names = Array("--rebalance-threshold"), description = "parameter x the average load gives rebalancing threshold for an attribute group")
    var rebalanceFactor = 1d
    @Parameter(names = Array("--rebalance-max-load"), description = "the maximum load that a (partition of a) join line may produce")
    var rebalanceMaxLoad = 10000 * 10000
    @Parameter(names = Array("--any-binary-captures"), description = "whether to create old-style join captures based on " +
      "unary frequent conditions only")
    var isCreateAnyBinaryCaptures = false
    @Parameter(names = Array("--find-frequent-captures"), description = "whether to find frequent captures for pruning")
    var isFindFrequentCaptures = false
    @Parameter(names = Array("--merge-window-size"), description = "number of elements that can be merged at once")
    var mergeWindowSize = -1
    @Parameter(names = Array("--find-only-fcs"), description = "if only frequent conditions shall be found")
    var findOnlyFrequentConditions = 0
    @Parameter(names = Array("--do-only-join"), description = "whether to leave out the search space traversal")
    var isOnlyJoin = false
    @Parameter(names = Array("--create-join-histogram"), description = "whether to leave out the search space traversal")
    var isCreateJoinHistogram = false
    @Parameter(names = Array("--debug-level"), description = "0: no debug prints, 1: some, ...")
    var debugLevel = DEBUG_LEVEL_SILENT
    @Parameter(names = Array("--print-plan"), description = "print out the execution plan")
    var isPrintExecutionPlan = false
    @Parameter(names = Array("--apply-hash"))
    var isApplyHash = false
    @Parameter(names = Array("--projection"), description = "what shall be used as projection for captures")
    var projectionAttributes = "spo"
    @Parameter(names = Array("--explicit-threshold"))
    var explicitCandidateThreshold = -1
    @Parameter(names = Array("--balanced-overlap-candidates"))
    var isBalanceOverlapCandidates = false
    @Parameter(names = Array("--hash-dictionary"))
    var isHashBasedDictionaryCompression: Boolean = false
    @Parameter(names = Array("--hash-function"))
    var hashAlgorithm: String = HashFunction.DEFAULT_ALGORITHM
    @Parameter(names = Array("--hash-bytes"))
    var hashBytes = -1
    @Parameter(names = Array("--sbf-bytes"), description = "number of bits per entry to use in spectral Bloom filters")
    var spectralBloomFilterBits = -1
    @Parameter(names = Array("--tabs"), description = "if input file is tab-separated")
    var isInputFileWithTabs = false
    @Parameter(names = Array("--only-read"), description = "if only the input files shall be read")
    var isOnlyRead = false
    @Parameter(names = Array("--counters"), description = "count statistics (0: none, 1: basic, 2: all)")
    var counterLevel = 0
  }

  val DEBUG_LEVEL_SILENT = 0
  val DEBUG_LEVEL_STATISTICS = 1
  val DEBUG_LEVEL_SANITY = 2
  val DEBUG_LEVEL_VERBOSE = 3


}
