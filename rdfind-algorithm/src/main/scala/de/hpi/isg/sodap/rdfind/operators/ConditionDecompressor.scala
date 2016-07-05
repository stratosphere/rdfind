package de.hpi.isg.sodap.rdfind.operators

import java.lang
import javassist.bytecode.stackmap.TypeTag

import de.hpi.isg.sodap.rdfind.util.HashCollisionHandler
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

/**
 * Created by basti on 5/19/15.
 */
class ConditionDecompressor[T](extractionFunc: T => String, replaceFunc: (T, String) => Unit)
  extends CoGroupFunction[T, (String, String), T] {

  override def coGroup(items: java.lang.Iterable[T],
                       dictionaryEntries: java.lang.Iterable[(String, String)],
                       out: Collector[T]) = {

    // Try to get the dictionary entry at first.
    val dictionaryEntryIterator = dictionaryEntries.iterator
    val dictionaryEntry = if (dictionaryEntryIterator.hasNext) dictionaryEntryIterator.next() else null
    if (dictionaryEntryIterator.hasNext) {
      throw new IllegalStateException(s"Unexpected second dictionary entry ${dictionaryEntryIterator.next()}.")
    }

    // Now replace all the item values.
    if (dictionaryEntry == null) {
      items.foreach { item =>
        val value = extractionFunc(item)
        if (HashCollisionHandler.isValue(value)) {
          replaceFunc(item, HashCollisionHandler.extractValue(value))
        }
        else if (HashCollisionHandler.isHash(value)) {
          throw new IllegalStateException(s"No dictionary entry for $value in $item.")
        } else {
          // Replace random empty values.
          replaceFunc(item, "")
        }
        out.collect(item)
      }
    } else {
      items.foreach { item =>
        replaceFunc(item, dictionaryEntry._2)
        out.collect(item)
      }
    }
  }
  

}
