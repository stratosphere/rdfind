package de.hpi.isg.sodap.rdfind.data

import com.google.common.hash.{Funnel => _Funnel, PrimitiveSink}
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._

/**
 * This class represents a general conditional inclusion dependency on RDF data. If a condition of dependent
 * or referenced capture is only unary, then the first value is set, the second is {@code null}.
 * @author sebastian.kruse 
 * @since 21.04.2015
 */
case class Cind(var depCaptureType: Int, var depConditionValue1: String, var depConditionValue2: String,
                var refCaptureType: Int, var refConditionValue1: String, var refConditionValue2: String,
                var support: Int = -1) {

  def update(depCapture: Condition = null, refCapture: Condition = null) = {
    if (depCapture != null) {
      this.depCaptureType = depCapture.conditionType
      this.depConditionValue1 = depCapture.conditionValue1
      this.depConditionValue2 = depCapture.conditionValue2
    }
    if (refCapture != null) {
      this.refCaptureType = refCapture.conditionType
      this.refConditionValue1 = refCapture.conditionValue1
      this.refConditionValue2 = refCapture.conditionValue2
    }
  }

  override def toString: String = s"${prettyPrint(depCaptureType, depConditionValue1, depConditionValue2)} < " +
    s"${prettyPrint(refCaptureType, refConditionValue1, refConditionValue2)} " +
    s"(${if (support == -1) "unknown support" else s"support=$support"})"

  def checkSanity() = {
    try {
      Condition(this.depConditionValue1, this.depConditionValue2, this.depCaptureType).checkSanity()
      Condition(this.refConditionValue1, this.refConditionValue2, this.refCaptureType).checkSanity()
    } catch {
      case e: Throwable => throw new IllegalStateException("Illegal CIND detected.", e)
    }
  }
}

object Cind {

  class Funnel extends _Funnel[Cind] {
    override def funnel(from: Cind, into: PrimitiveSink): Unit = into
      .putInt(from.depCaptureType)
      .putUnencodedChars(from.depConditionValue1)
      .putUnencodedChars(if (from.depConditionValue2 != null) from.depConditionValue2 else "")
      .putInt(from.refCaptureType)
      .putUnencodedChars(from.refConditionValue1)
      .putUnencodedChars(if (from.refConditionValue2 != null) from.refConditionValue2 else "")

    override def equals(obj: scala.Any): Boolean = obj != null && obj.isInstanceOf[Funnel]

    override def hashCode(): Int = 923412
  }


}