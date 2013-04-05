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

import org.infinispan.Cache
import org.infinispan.stats.Stats
import org.jboss.netty.buffer.ChannelBuffer
import org.infinispan.server.core.{RequestParameters, CacheValue}
import org.infinispan.server.core.transport.NettyTransport

/**
 * This class represents the work to be done by a decoder of a particular Hot Rod protocol version.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractVersionedDecoder {

   /**
    * Having read the message's Id, read the rest of Hot Rod header from the given buffer and return it.
    */
   def readHeader(buffer: ChannelBuffer, version: Byte, messageId: Long, header: HotRodHeader): Boolean

   /**
    * Read the key to operate on from the message.
    */
   def readKey(header: HotRodHeader, buffer: ChannelBuffer): (Array[Byte], Boolean)

   /**
    * Read the parameters of the operation, if present.
    */
   def readParameters(header: HotRodHeader, buffer: ChannelBuffer): (RequestParameters, Boolean)

   /**
    * Read the value part of the operation.
    */
   def createValue(params: RequestParameters, nextVersion: Long, rawValue: Array[Byte]): CacheValue

   /**
    * Create a successful response.
    */
   def createSuccessResponse(header: HotRodHeader, prev: CacheValue): AnyRef

   /**
    * Create a response indicating the the operation could not be executed.
    */
   def createNotExecutedResponse(header: HotRodHeader, prev: CacheValue): AnyRef

   /**
    * Create a response indicating that the key, which the message tried to operate on, did not exist.
    */
   def createNotExistResponse(header: HotRodHeader): AnyRef

   /**
    * Create a response for get a request.
    */
   def createGetResponse(header: HotRodHeader, v: CacheValue): AnyRef

   /**
    * Handle a protocol specific header reading.
    */
   def customReadHeader(header: HotRodHeader, buffer: ChannelBuffer, cache: Cache[Array[Byte], CacheValue]): AnyRef

   /**
    * Handle a protocol specific key reading.
    */
   def customReadKey(header: HotRodHeader, buffer: ChannelBuffer, cache: Cache[Array[Byte], CacheValue]): AnyRef

   /**
    * Handle a protocol specific value reading.
    */
   def customReadValue(header: HotRodHeader, buffer: ChannelBuffer, cache: Cache[Array[Byte], CacheValue]): AnyRef

   /**
    * Create a response for the stats command.
    */
   def createStatsResponse(header: HotRodHeader, stats: Stats, t: NettyTransport): AnyRef

   /**
    * Create an error response based on the Throwable instance received.
    */
   def createErrorResponse(header: HotRodHeader, t: Throwable): ErrorResponse

   /**
    * Get an optimized cache instance depending on the operation parameters.
    */
   def getOptimizedCache(h: HotRodHeader, c: Cache[Array[Byte], CacheValue]): Cache[Array[Byte], CacheValue]
}
