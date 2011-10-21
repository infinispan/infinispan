/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

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
   def writeHeader(r: Response, buf: ChannelBuffer, addressCache: Cache[Address, ServerAddress])

   /**
    * Write operation response using the given channel buffer
    */
   def writeResponse(r: Response, buf: ChannelBuffer, cacheManager: EmbeddedCacheManager)

}