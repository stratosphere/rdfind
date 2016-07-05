package de.hpi.isg.sodap.rdfind.util

import java.security.MessageDigest

import scala.util.Random

/**
 * @author sebastian.kruse 
 * @since 18.05.2015
 */
class HashCollisionHandler(hashFunction: HashFunction, collisionHashes: Set[String]) {

  lazy val random = new Random()

  def hashAndResolveCollision(value: String) = resolveCollsion(this.hashFunction.hashStringToString(value), value)

  def resolveCollsion(hash: String, originalValue: String) =
    if (this.collisionHashes.contains(hash)) "~" + originalValue else "#" + hash

  def randomReplaceNoValue(value: String) =
    if (value == null || value.isEmpty())
      HashCollisionHandler.PRINTABLE_CHARS(this.random.nextInt(HashCollisionHandler.PRINTABLE_CHARS.length)) +
        HashCollisionHandler.PRINTABLE_CHARS(this.random.nextInt(HashCollisionHandler.PRINTABLE_CHARS.length))
    else
      value
}

object HashCollisionHandler {

  val PRINTABLE_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".split("")

  def isHash(value: String) = !value.isEmpty && value.charAt(0) == '#'

  def isValue(value: String) = !value.isEmpty && value.charAt(0) == '~'

  def extractValue(value: String) = value.substring(1)

  def escapeHash(rawHash: String) = "#" + rawHash

  def escapValue(rawValue: String) = "~" + rawValue

}

