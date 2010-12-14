package org.infinispan.server.core

/**
 * Externalizer ids used by Server module {@link Externalizer} implementations.
 *
 * Information about the valid id range can be found <a href="http://community.jboss.org/docs/DOC-16198">here</a>
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
object ExternalizerIds {

   val SERVER_CACHE_VALUE = 1100
   val MEMCACHED_CACHE_VALUE = 1101
   val TOPOLOGY_ADDRESS = 1102
   val TOPOLOGY_VIEW = 1103

}