/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.hotrod

import logging.Log
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.Cache
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.Channel
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import org.infinispan.remoting.transport.Address
import org.infinispan.util.Util

/**
 * Hot Rod specific encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodEncoder(cacheManager: EmbeddedCacheManager, server: HotRodServer)
        extends OneToOneEncoder with Constants with Log {

   private lazy val isClustered: Boolean = cacheManager.getGlobalConfiguration.getTransportClass != null
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
