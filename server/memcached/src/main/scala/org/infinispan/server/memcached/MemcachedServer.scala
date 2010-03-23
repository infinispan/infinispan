package org.infinispan.server.memcached

import org.infinispan.server.core.AbstractProtocolServer
import org.infinispan.server.core.transport.{Decoder, Encoder}
import org.infinispan.manager.CacheManager

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class MemcachedServer extends AbstractProtocolServer {

   protected override def getEncoder: Encoder = null

   protected override def getDecoder(cacheManager: CacheManager): Decoder = new MemcachedDecoder(cacheManager)

}