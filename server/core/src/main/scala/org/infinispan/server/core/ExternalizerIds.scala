package org.infinispan.server.core

/**
 * Externalizer ids used by Server module {@link AdvancedExternalizer} implementations.
 *
 * Information about the valid id range can be found <a href="http://community.jboss.org/docs/DOC-16198">here</a>
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
object ExternalizerIds {

   val SERVER_ENTRY_VERSION = 1100
   val MEMCACHED_METADATA = 1101
   val TOPOLOGY_ADDRESS = 1102
   val TOPOLOGY_VIEW = 1103
   val SERVER_ADDRESS = 1104
   val MIME_METADATA = 1105
   val BINARY_FILTER = 1106
   val BINARY_CONVERTER = 1107

}