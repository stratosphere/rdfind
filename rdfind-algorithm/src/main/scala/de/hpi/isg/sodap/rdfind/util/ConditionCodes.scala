package de.hpi.isg.sodap.rdfind.util

/**
 * This object provides utility methods for the operation with condition codes as used in the RDFind project.
 * Primary conditions are used to specify triple elements that are restricted with a condition. The secondary
 * conditions express the fields selected for an inclusion dependency.
 *
 * @author Sebastian
 * @since 08.04.2015.
 */
object ConditionCodes {
  val subjectCondition = 1
  val predicateCondition = 2
  val objectCondition = 4
  val numTypeBits = 3
  val typeBitMask = 7
  // = 0000 0111
  val subjectPredicateCondition = subjectCondition | predicateCondition
  val subjectObjectCondition = subjectCondition | objectCondition
  val predicateObjectCondition = predicateCondition | objectCondition
  val subjectPredicateConditionWithSelection = addSecondaryConditions(subjectPredicateCondition)
  val subjectObjectConditionWithSelection = addSecondaryConditions(subjectObjectCondition)
  val predicateObjectConditionWithSelection = addSecondaryConditions(predicateObjectCondition)

  private val codeToChar = Map(subjectCondition -> "s", predicateCondition -> "p", objectCondition -> "o")

  def merge(code1: Int, code2: Int): Int = code1 | code2

  def extractPrimaryConditions(conditionCode: Int) = conditionCode & typeBitMask

  def extractSecondaryConditions(conditionCode: Int) = (conditionCode >> numTypeBits) & typeBitMask

  /**
   * Add secondary conditions that are not already primary conditions.
   *
   * @param conditionCode is the bitmask containing primary conditions
   */
  def addSecondaryConditions(conditionCode: Int) = (conditionCode & typeBitMask) |
    ((~conditionCode & typeBitMask) << numTypeBits)

  /**
   * Adds a secondary condition that is not a primary condition, yet.
   *
   * @param conditionCode is the bitmask containing primary condition
   */
  def addFirstSecondaryCondition(conditionCode: Int) = {
    val primaryCondition = extractPrimaryConditions(conditionCode)
    val unusedConditons = typeBitMask ^ conditionCode
    createConditionCode(primaryCondition, secondaryCondition = Integer.lowestOneBit(unusedConditons))
  }

  /**
   * Adds another secondary condition that is not a primary condition, yet.
   *
   * @param conditionCode is the bitmask containing primary condition
   */
  def addSecondSecondaryCondition(conditionCode: Int) = {
    val primaryCondition = extractPrimaryConditions(conditionCode)
    val unusedConditons = typeBitMask ^ conditionCode
    val firstSecondaryCondition = Integer.lowestOneBit(unusedConditons)
    createConditionCode(primaryCondition, secondaryCondition = unusedConditons & ~firstSecondaryCondition)
  }

  /**
   * Splits a given condition code with two expected primary conditions into a code with the first primary
   * condition, a code with the second primary condition, and a code with the unused primary condition.
   * @param conditionCode is the code to split
   * @return a tuple containing the above mentioned conditions
   */
  def decodeConditionCode(conditionCode: Int, isRequireDoubleCode: Boolean = false) = {
    // Find the correct condition and condition type.
    val firstConditionType = java.lang.Integer.lowestOneBit(conditionCode)
    val secondConditionType = java.lang.Integer.lowestOneBit(conditionCode & ~firstConditionType)
    val freeConditionType = ~firstConditionType & ~secondConditionType & 7

    if (isRequireDoubleCode && secondConditionType == 0)
      throw new IllegalArgumentException(s"Cannot split $conditionCode")
    (firstConditionType, secondConditionType, freeConditionType)
  }

  def createConditionCode(firstPrimaryCondition: Int, secondPrimaryCondition: Int = 0, secondaryCondition: Int = 0) =
    ((firstPrimaryCondition | secondPrimaryCondition) & typeBitMask) |
      ((secondaryCondition & typeBitMask) << numTypeBits)

  def isSubcode(candidate: Int, superCode: Int) = (candidate & superCode) == candidate

  /** Can also be applied to capture codes. */
  def isBinaryCondition(conditionCode: Int) = Integer.bitCount(conditionCode & typeBitMask) == 2

  /** Can also be applied to capture codes. */
  def isUnaryCondition(conditionCode: Int) = Integer.bitCount(conditionCode & typeBitMask) == 1

  def removePrimaryCondition(captureCode: Int) = captureCode & ~typeBitMask

  def extractFirstSubcapture(captureCode: Int) = removePrimaryCondition(captureCode) | java.lang.Integer.lowestOneBit(captureCode)

  def extractSecondSubcapture(captureCode: Int) = removePrimaryCondition(captureCode) | {
    val firstConditionType = java.lang.Integer.lowestOneBit(captureCode)
    java.lang.Integer.lowestOneBit(captureCode & ~firstConditionType)
  }

  def prettyPrint(captureCode: Int, value1: String, value2: String = null): String = {
    val projectionChar = codeToChar.getOrElse(extractSecondaryConditions(captureCode), "")
    val selections = decodeConditionCode(extractPrimaryConditions(captureCode))
    if (selections._2 == 0) s"$projectionChar[${codeToChar(selections._1)}=$value1]"
    else s"$projectionChar[${codeToChar(selections._1)}=$value1,${codeToChar(selections._2)}=$value2]"
  }

  def isValidStandardCapture(conditionCode: Int): Boolean = {
    val primaryConditions = extractPrimaryConditions(conditionCode)
    // One or two primary conditions?
    val numPrimaryConditons = java.lang.Integer.bitCount(primaryConditions)
    if (numPrimaryConditons < 1 || numPrimaryConditons > 2)
    return false

    // One secondary condition?
    val secondaryConditions = extractSecondaryConditions(conditionCode)
    val numSecondaryConditons = java.lang.Integer.bitCount(secondaryConditions)
    if (numSecondaryConditons != 1)
      return false

    // Primary and secondary conditions disjoint?
    val invalidConditions = primaryConditions & secondaryConditions
    if (invalidConditions != 0)
      return false

    // No other bits set?
    (~0x3F & conditionCode) == 0
  }
}
