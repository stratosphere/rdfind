package de.hpi.isg.sodap.rdfind.data

/**
 * Created by basti on 5/28/15.
 */
trait CandidateSet {

  def depCount: Int

  def depCondition: Condition

}
