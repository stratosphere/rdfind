package de.hpi.isg.sodap.rdfind.operators.candidate_merging

import java.lang.Iterable

import de.hpi.isg.sodap.rdfind.data._
import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.runtime.ScalaRunTime

/**
 * Created by basti on 5/28/15.
 * @tparam T is the type of candidate sets to be merged
 * @tparam U is the right-hand side type of the dependencies
 */
abstract class BulkMergeDependencies[T <: CandidateSet, U: ClassTag](windowSize: Int = -1,
                                                                     countMergeFunction: (Int, Int) => Int = _ + _)
  extends GroupCombineFunction[T, T]
  with GroupReduceFunction[T, T] {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  protected lazy val exactWindow = new ArrayBuffer[Array[U]](if (windowSize != -1) windowSize else 100)

  private lazy val priorityQueue = mutable.PriorityQueue[(U, Int)]()(priorityQueueOrdering)

  private var freeMemoryCheckCounter = 0

  private lazy val iterationIndices = new ArrayBuffer[Int]()

  private lazy val aggregationBuffer = new ArrayBuffer[U]()

  private var _isCombining = false

  protected def isCombining = this._isCombining

  protected def priorityQueueOrdering: Ordering[(U, Int)]

  override def combine(values: Iterable[T], out: Collector[T]): Unit = {
    this._isCombining = true
    reduce(values, out)
  }

  override def reduce(candidates: Iterable[T], out: Collector[T]): Unit = {

    var counter = 0

    // We will need the dependent side.
    var dependentCondition: Condition = null
    var depCount = 0

    // Process the candidates window-wise.
    val candidateIterator = candidates.iterator()
    while (candidateIterator.hasNext) {

      // Fill the window.
      this.freeMemoryCheckCounter = 0
      while (!isWindowFull && candidateIterator.hasNext) {
        val freeMem = java.lang.Runtime.getRuntime.freeMemory() / 1024 / 1024
        val candidate: T =
          try {
            candidateIterator.next()
          } catch {
            case e: Throwable => {
              throw new RuntimeException(s"Failed with $freeMem MiB of free memory in ${getClass.getSimpleName} " +
                s"(key: $dependentCondition, " +
                s"window base: ${if (this.exactWindow.isEmpty) null else ScalaRunTime.stringOf(this.exactWindow(0))}).", e)
            }
          }
        addToWindow(candidate)
        counter += 1
        if (dependentCondition == null)
          dependentCondition = candidate.depCondition
        depCount = countMergeFunction(candidate.depCount, depCount)
      }

      mergeWindow()
    }

    outputResults(out, dependentCondition, depCount)
    cleanUp()
  }

  /**
   * Add the given candidates to the window in this iteration.
   */
  def addToWindow(candidates: T)

  /**
   * Tells if yet more candidates can be added to the window.
   */
  def isWindowFull: Boolean = this.exactWindow.size > 1 && {
    this.freeMemoryCheckCounter += 1
    if (this.freeMemoryCheckCounter > 100) {
      this.freeMemoryCheckCounter = 0
      val freeMemoryMiB = Runtime.getRuntime.freeMemory() / 1024 / 1024
      logger.trace(s"Free memory ~${freeMemoryMiB} MiB.")
      freeMemoryMiB < 50
    } else false
  } && this.windowSize != -1 && this.exactWindow.size >= this.windowSize

  def mergeWindow(): Unit = {
    // Check that there is actually work to do.
    if (this.exactWindow.size <= 1) return

    // Initialize the window aggregation: take the first condition in each list.
    for (i <- 0 until this.exactWindow.size; condition = this.exactWindow(i)) {
      this.iterationIndices += 0
      if (condition.length > 0) {
        val priorityQueueEntry = (condition(0), i)
        this.priorityQueue += priorityQueueEntry
      }
    }

    while (this.priorityQueue.size >= minCandidateSize) {
      prepareForMergeGroup()

      // Take the top elements.
      val headElement = this.priorityQueue.head
      while (this.priorityQueue.nonEmpty && priorityQueueOrdering.equiv(this.priorityQueue.head, headElement)) {
        // Step forward where we found the top element.
        val windowElementIndex = this.priorityQueue.dequeue()._2
        val windowElement = this.exactWindow(windowElementIndex)
        val nextIndex = this.iterationIndices(windowElementIndex) + 1

        noticeMergeGroupMember(windowElement(nextIndex - 1))

        if (nextIndex < windowElement.length) {
          val newPriorityQueueEntry = (windowElement(nextIndex), windowElementIndex)
          this.priorityQueue += newPriorityQueueEntry
          this.iterationIndices(windowElementIndex) = nextIndex
        }
      }

      addMergedGroup(this.aggregationBuffer)
    }

    // Put the aggregation result in the window for either following iterations or the result evalution.
    this.exactWindow.clear()
    this.exactWindow += this.aggregationBuffer.toArray

    // Clean up the merge utilities.
    this.priorityQueue.clear()
    this.iterationIndices.clear()
    this.aggregationBuffer.clear()

    this._isCombining = false
  }

  /** Tells the number of elements that must still be mergeable in the window. */
  def minCandidateSize: Int

  def prepareForMergeGroup()

  def noticeMergeGroupMember(member: U)

  def addMergedGroup(collector: mutable.Buffer[U])

  def outputResults(out: Collector[T], depCondition: Condition, depCount: Int)

  def cleanUp() = this.exactWindow.clear()
}

object BulkMergeDependencies {

  class PriorityQueueOrdering[T <: Ordered[T]] extends Ordering[(T, Int)] {
    override def compare(x: (T, Int), y: (T, Int)): Int = y._1.compare(x._1)
  }


  /**
   * Orders tuples ([[Condition]], [[Int]]) by the condition in descending order.
   */
  object ConditionPriorityQueueOrdering extends PriorityQueueOrdering[Condition]

  /**
   * Orders tuples ([[ConditionCount]], [[Int]]) by the condition in descending order.
   */
  object ConditionCountPriorityQueueOrdering extends PriorityQueueOrdering[ConditionCount]

}
