package org.infinispan.server.hotrod

import logging.Log
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.Cache
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.Channel
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import org.infinispan.remoting.transport.Address
import org.infinispan.commons.util.Util

/**
 * Hot Rod specific encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodEncoder(cacheManager: EmbeddedCacheManager, server: HotRodServer)
        extends OneToOneEncoder with Constants with Log {

   private lazy val isClustered: Boolean = cacheManager.getCacheManagerConfiguration.transport.transport  != null
   private lazy val addressCache: Cache[Address, ServerAddress] =
      if (isClustered) cacheManager.getCache(server.getConfiguration.topologyCacheName) else null
   private val isTrace = isTraceEnabled

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: AnyRef): AnyRef = {
      trace("Encode msg %s", msg)

      // Safe cast
      val r = msg.asInstanceOf[Response]
      val buf = dynamicBuffer
      val encoder = r.version match {
         case VERSION_10 => Encoders.Encoder10
         case VERSION_11 => Encoders.Encoder11
         case VERSION_12 => Encoders.Encoder12
         case 0 => Encoders.Encoder12
      }

      r.version match {
         case VERSION_10 | VERSION_11 | VERSION_12 => encoder.writeHeader(r, buf, addressCache, server)
         // if error before reading version, don't send any topology changes
         // cos the encoding might vary from one version to the other
         case 0 => encoder.writeHeader(r, buf, null, null)
      }

      encoder.writeResponse(r, buf, cacheManager, server)
      if (isTrace)
         trace("Write buffer contents %s to channel %s",
            Util.hexDump(buf.toByteBuffer), ctx.getChannel)

      buf
   }

}
