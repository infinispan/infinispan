package org.infinispan.server.hotrod

import org.infinispan.manager.EmbeddedCacheManager
import io.netty.buffer.ByteBuf
import org.infinispan.server.hotrod.Events.Event

/**
 * This class represents the work to be done by an encoder of a particular
 * Hot Rod protocol version.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
abstract class AbstractVersionedEncoder {

   /**
    * Write the header to the given channel buffer
    */
   def writeHeader(r: Response, buf: ByteBuf, addressCache: AddressCache, server: HotRodServer)

   /**
    * Write operation response using the given channel buffer
    */
   def writeResponse(r: Response, buf: ByteBuf, cacheManager: EmbeddedCacheManager, server: HotRodServer)

   /**
    * Write an event, including its header, using the given channel buffer
    */
   def writeEvent(e: Event, buf: ByteBuf)

}
