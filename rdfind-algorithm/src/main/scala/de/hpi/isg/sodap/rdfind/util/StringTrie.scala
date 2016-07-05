package de.hpi.isg.sodap.rdfind.util

import de.hpi.isg.sodap.rdfind.util.StringTrie.Entry

import scala.annotation.tailrec
import scala.collection.mutable

class StringTrie[T] {

  private val root = Entry[T]("")

  private var isSquashed = false

  /** Add a new key-value pair into the trie. */
  def +=(keyValuePair: (String, T)): Unit = {
    if (this.isSquashed) throw new IllegalStateException("Cannot add to finalized trie.")
    val key = keyValuePair._1
    val value = keyValuePair._2

    // Traverse as deep as possible at first.
    var entry = this.root
    var nextEntry: Entry[T] = null
    var position = 0
    while (position < key.length && { nextEntry = entry.children.getOrElse(key.charAt(position), null); nextEntry != null }) {
      position += 1
      entry = nextEntry
    }

    // Add futher nodes as needed.
    while (position < key.length) {
      val newEntry = Entry[T](key.charAt(position).toString)
      entry.children += key.charAt(position) -> newEntry
      entry = newEntry
      position += 1
    }

    if (entry.value != null) {
      throw new IllegalArgumentException(s"Key already exists: $key.")
    }
    entry.value = value
  }

  /**
   * Return the value associated with the longest key that is a prefix of the given key or {@code null} if no such key
   * exists. 
   */
  def apply(key: String): T = {
    val keyAndValue = getKeyAndValue(key)
    if (keyAndValue == null) null.asInstanceOf[T] else keyAndValue._2
  }

  def getKeyAndValue(key: String): (String, T) = {
    searchKey(key, 0, this.root, null)
  }

  def squash(): Unit = {
    if (this.isSquashed) return
    this.root.squash()
    this.isSquashed = true
  }

  @tailrec
  private def searchKey(key: String, keyPos: Int, entry: Entry[T], bestMatch: (String, T)): (String, T) = {
    // Check if the node we arrived at has a matching key.
    if (key.length - keyPos < entry.key.length ||
      (0 until entry.key.length).exists(i => key.charAt(keyPos + i) != entry.key.charAt(i)))
      bestMatch

    else {
      // Check if the new node we arrived at can provide a new key.
      val newBestMatch = if (entry.value != null) (key.substring(0, keyPos + entry.key.length), entry.value) else bestMatch

      // If we have used up all the key, stop.
      if (keyPos + entry.key.length >= key.length)
        newBestMatch

      // Try to find a next entry for the current key pos.
      else {
        val nextChar = key.charAt(keyPos + entry.key.length)
        val nextEntry = entry.children.getOrElse(nextChar, null)

        // For squashed entries, check its complete key matches the input key.
        if (nextEntry == null)
          newBestMatch
        else
          searchKey(key, keyPos + entry.key.length, nextEntry, newBestMatch)
      }
    }

  }

}

object StringTrie {

  case class Entry[T](var key: String,
                      var value: T = null.asInstanceOf[T],
                      var children: mutable.Map[Char, Entry[T]] = mutable.Map[Char, Entry[T]]()) {

    /**
     * Squash the subtree starting at this entry.
     */
    def squash(): Unit = {
      // Squash the children at first.
      this.children.valuesIterator.foreach(_.squash())

      // Squash this node if it has a) no value and b) only one child.
      if (this.value == null && this.children.size == 1) {
        // Merge in the only child.
        val child = this.children.valuesIterator.next()
        this.key += child.key
        this.value = child.value
        this.children = child.children
      }
    }
  }

}