package de.hpi.isg.sodap.rdfind.util

import java.security.MessageDigest

import org.apache.commons.codec.binary


/**
 * @author sebastian.kruse 
 * @since 18.05.2015
 */
class HashFunction(algorithm: String = "MD5", maxBytes: Int = -1, isUnsetFirstMsb: Boolean = false) {

  lazy val messageDigest = MessageDigest.getInstance(algorithm)

  lazy val base64 = new binary.Base64

  def hashStringToString(string: String): String = {
    // Do the hashing.
    messageDigest.reset()
    val digest = messageDigest.digest(string.getBytes)

    // Manipulate the hash if requested.
    if (this.isUnsetFirstMsb) {
      digest(0) = (digest(0) & 0x7F).asInstanceOf[Byte]
    }

    // Base 64
    //    val result = new String(this.base64.encode(digest), "UTF-8")

    // (Base 128)--: unset the first byte
    for (i <- 0 until digest.length)
      digest(i) = (digest(i) & 0x7F).asInstanceOf[Byte]
    new String(digest, "ASCII")
  }

}


object HashFunction {
  val DEFAULT_ALGORITHM = "MD5"
}


