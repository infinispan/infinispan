package org.infinispan.server.hotrod

import org.jboss.netty.buffer.ChannelBuffer
import org.infinispan.Cache
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.remoting.transport.Address

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
   def writeHeader(r: Response, buf: ChannelBuffer,
         addressCache: Cache[Address, ServerAddress], server: HotRodServer)

   /**
    * Write operation response using the given channel buffer
    */
   def writeResponse(r: Response, buf: ChannelBuffer, cacheManager: EmbeddedCacheManager, server: HotRodServer)

}
