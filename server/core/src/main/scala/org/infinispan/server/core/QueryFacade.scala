package org.infinispan.server.core

import org.infinispan.AdvancedCache

/**
 * Query facade
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
trait QueryFacade {

   def query(cache: AdvancedCache[Array[Byte], Array[Byte]], query: Array[Byte]): Array[Byte]

}
