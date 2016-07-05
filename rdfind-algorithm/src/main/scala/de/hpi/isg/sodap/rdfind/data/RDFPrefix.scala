package de.hpi.isg.sodap.rdfind.data

/**
 * Describes an RDF prefix, e.g, "dbpedia" -> "http://dbpedia.org/resource/"
 *
 * @author sebastian.kruse 
 * @since 18.06.2015
 */
case class RDFPrefix(prefix: String, url: String)
