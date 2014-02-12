package org.infinispan.server.hotrod

import org.infinispan.Cache
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.remoting.transport.Address
import io.netty.buffer.ByteBuf

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
   def writeHeader(r: Response, buf: ByteBuf,
         addressCache: Cache[Address, ServerAddress], server: HotRodServer)

   /**
    * Write operation response using the given channel buffer
    */
   def writeResponse(r: Response, buf: ByteBuf, cacheManager: EmbeddedCacheManager, server: HotRodServer)

}
