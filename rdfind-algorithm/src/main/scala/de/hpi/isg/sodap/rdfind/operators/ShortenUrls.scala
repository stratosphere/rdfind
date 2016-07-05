package de.hpi.isg.sodap.rdfind.operators

import java.lang.Iterable

import de.hpi.isg.sodap.rdfind.data.{RDFPrefix, RDFTriple}
import de.hpi.isg.sodap.rdfind.util.StringTrie
import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichMapFunction}
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConversions._

/**
 * @author sebastian.kruse 
 * @since 18.06.2015
 */
class ShortenUrls extends RichMapFunction[RDFTriple, RDFTriple] {

  var prefixTrie: StringTrie[String] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    this.prefixTrie = getRuntimeContext.getBroadcastVariableWithInitializer(
      ShortenUrls.PREFIX_BROADCAST,
      ShortenUrls.PrefixTrieCreator)
  }

  override def map(triple: RDFTriple): RDFTriple = {
    triple.subj = shorten(triple.subj)
    triple.pred = shorten(triple.pred)
    triple.obj = shorten(triple.obj)
    triple
  }

  @inline
  private def shorten(url: String): String = {
    if (url.endsWith(">")) {
      val keyValuePair = this.prefixTrie.getKeyAndValue(url)
      if (keyValuePair != null) {
        return keyValuePair._2 + url.substring(keyValuePair._1.length, url.length - 1)
      }
    }
    url
  }
}


object ShortenUrls {

  val PREFIX_BROADCAST = "prefixes"

  object PrefixTrieCreator extends BroadcastVariableInitializer[RDFPrefix, StringTrie[String]] {
    override def initializeBroadcastVariable(data: Iterable[RDFPrefix]): StringTrie[String] = {
      val trie = new StringTrie[String]
      data.foreach(prefix => trie += s"<${prefix.url}" -> s"${prefix.prefix}:")
      trie.squash()
      trie
    }
  }

}
