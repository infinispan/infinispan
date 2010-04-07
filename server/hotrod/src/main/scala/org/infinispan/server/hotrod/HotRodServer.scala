package org.infinispan.server.hotrod

import org.infinispan.manager.CacheManager
import org.infinispan.server.core.AbstractProtocolServer
import org.infinispan.server.core.transport.{Decoder, Encoder}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class HotRodServer extends AbstractProtocolServer {

   override def getEncoder: Encoder = new HotRodEncoder

   override def getDecoder: Decoder = new HotRodDecoder(getCacheManager)

}