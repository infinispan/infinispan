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

import org.infinispan.server.core._
import transport._
import OperationStatus._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.hotrod.ProtocolFlag._
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import java.nio.channels.ClosedChannelException
import org.infinispan.Cache
import org.infinispan.util.ByteArrayKey
import java.io.{IOException, StreamCorruptedException}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.Channel
import java.lang.StringBuilder
import java.util.HashSet
import org.infinispan.util.concurrent.ConcurrentMapFactory

/**
 * Top level Hot Rod decoder that after figuring out the version, delegates the rest of the reading to the
 * corresponding versioned decoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodDecoder(cacheManager: EmbeddedCacheManager, transport: NettyTransport, server: HotRodServer)
        extends AbstractProtocolDecoder[ByteArrayKey, CacheValue](transport) with Constants {
   type SuitableHeader = HotRodHeader

   type SuitableParameters = RequestParameters
   private var isError = false

   private val isTrace = isTraceEnabled

   override def readHeader(buffer: ChannelBuffer): (Option[HotRodHeader], Boolean) = {
      try {
         val magic = buffer.readUnsignedByte
         if (magic != MAGIC_REQ) {
            if (!isError) {
               throw new InvalidMagicIdException("Error reading magic byte or message id: " + magic)
            } else {
               trace("Error happened previously, ignoring %d byte until we find the magic number again", magic)
               return (None, false) // Keep trying to read until we find magic
            }
         }
      } catch {
         case e: Exception => {
            isError = true
            throw e
         }
      }

      val messageId = readUnsignedLong(buffer)
      val version = buffer.readUnsignedByte.toByte

      try {
         val decoder = version match {
            case VERSION_10 | VERSION_11 => Decoder10
            case _ => throw new UnknownVersionException(
               "Unknown version:" + version, version, messageId)
         }
         val (header, endOfOp) = decoder.readHeader(buffer, version, messageId)
         if (isTrace) trace("Decoded header %s", header)
         isError = false
         (Some(header), endOfOp)
      } catch {
         case e: HotRodUnknownOperationException => {
            isError = true
            throw e
         }
         case e: Exception => {
            isError = true
            throw new RequestParsingException(
               "Unable to parse header", version, messageId, e)
         }
      }
   }

   override def getCache: Cache[ByteArrayKey, CacheValue] = {
      val cacheName = header.cacheName
      // Talking to the wrong cache are really request parsing errors
      // and hence should be treated as client errors
      if (cacheName == HotRodServer.ADDRESS_CACHE_NAME)
         throw new RequestParsingException(
            "Remote requests are not allowed to topology cache. Do no send remote requests to cache '%s'".format(HotRodServer.ADDRESS_CACHE_NAME),
            header.version, header.messageId)

      var seenForFirstTime = false;
      // Try to avoid calling cacheManager.getCacheNames() if possible, since this creates a lot of unnecessary garbage
      if (server.isCacheNameKnown(cacheName)) {
         if (!(cacheManager.getCacheNames contains cacheName)) {
            throw new CacheNotFoundException(
               "Cache with name '%s' not found amongst the configured caches".format(cacheName),
               header.version, header.messageId)
         } else {
            seenForFirstTime = true;
         }
      }

      server.getCacheInstance(cacheName, cacheManager, seenForFirstTime)
   }

   override def readKey(b: ChannelBuffer): (ByteArrayKey, Boolean) =
      header.decoder.readKey(header, b)

   override def readParameters(ch: Channel, b: ChannelBuffer): Boolean = {
      val (parameters, endOfOp) = header.decoder.readParameters(header, b)
      params = parameters
      endOfOp
   }

   override protected def readValue(b: ChannelBuffer) {
      b.readBytes(rawValue)
   }

   override def createValue(nextVersion: Long): CacheValue =
      header.decoder.createValue(params, nextVersion, rawValue)

   override def createSuccessResponse(prev: CacheValue): AnyRef =
      header.decoder.createSuccessResponse(header, prev)

   override def createNotExecutedResponse(prev: CacheValue): AnyRef =
      header.decoder.createNotExecutedResponse(header, prev)

   override def createNotExistResponse: AnyRef =
      header.decoder.createNotExistResponse(header)

   override def createGetResponse(k: ByteArrayKey, v: CacheValue): AnyRef =
      header.decoder.createGetResponse(header, v)

   override def createMultiGetResponse(pairs: Map[ByteArrayKey, CacheValue]): AnyRef =
      null // Unsupported

   override protected def customDecodeHeader(ch: Channel, buffer: ChannelBuffer): AnyRef =
      writeResponse(ch, header.decoder.customReadHeader(header, buffer, cache))

   override protected def customDecodeKey(ch: Channel, buffer: ChannelBuffer): AnyRef =
      writeResponse(ch, header.decoder.customReadKey(header, buffer, cache))

   override protected def customDecodeValue(ch: Channel, buffer: ChannelBuffer): AnyRef =
      writeResponse(ch, header.decoder.customReadValue(header, buffer, cache))

   override def createStatsResponse: AnyRef =
      header.decoder.createStatsResponse(header, cache.getAdvancedCache.getStats, transport)

   override def createErrorResponse(t: Throwable): AnyRef = {
      t match {
         case h: HotRodException => h.response
         case c: ClosedChannelException => null
         case t: Throwable => {
            logErrorBeforeReadingRequest(t)
            new ErrorResponse(0, 0, "", 1, ServerError, 0, t.toString)
         }
      }
   }

   override protected def getOptimizedCache(c: Cache[ByteArrayKey, CacheValue]): Cache[ByteArrayKey, CacheValue] =
      header.decoder.getOptimizedCache(header, c)

   override protected def createServerException(e: Exception, b: ChannelBuffer): (HotRodException, Boolean) = {
      e match {
         case i: InvalidMagicIdException => {
            logExceptionReported(i)
            (new HotRodException(new ErrorResponse(
                  0, 0, "", 1, InvalidMagicOrMsgId, 0, i.toString), e), true)
         }
         case e: HotRodUnknownOperationException => {
            logExceptionReported(e)
            (new HotRodException(new ErrorResponse(
                  e.version, e.messageId, "", 1, UnknownOperation, 0, e.toString), e), true)
         }
         case u: UnknownVersionException => {
            logExceptionReported(u)
            (new HotRodException(new ErrorResponse(
                  u.version, u.messageId, "", 1, UnknownVersion, 0, u.toString), e), true)
         }
         case r: RequestParsingException => {
            logExceptionReported(r)
            val msg =
               if (r.getCause == null)
                  r.toString
               else
                  "%s: %s".format(r.getMessage, r.getCause.toString)
            (new HotRodException(new ErrorResponse(
                  r.version, r.messageId, "", 1, ParseError, 0, msg), e), true)
         }
         case i: IllegalStateException => {
            // Some internal server code could throw this, so make sure it's logged
            logExceptionReported(i)
            (new HotRodException(header.decoder.createErrorResponse(header, i), e), false)
         }
         case t: Throwable => (new HotRodException(header.decoder.createErrorResponse(header, t), e), false)
      }
   }
}

class UnknownVersionException(reason: String, val version: Byte, val messageId: Long)
        extends StreamCorruptedException(reason)

class HotRodUnknownOperationException(reason: String, val version: Byte, val messageId: Long)
        extends UnknownOperationException(reason)

class InvalidMagicIdException(reason: String) extends StreamCorruptedException(reason)

class RequestParsingException(reason: String, val version: Byte, val messageId: Long, cause: Exception)
        extends IOException(reason, cause) {
   def this(reason: String, version: Byte, messageId: Long) = this(reason, version, messageId, null)
}

class HotRodHeader(override val op: Enumeration#Value, val version: Byte,
                   val messageId: Long, val cacheName: String,
                   val flag: ProtocolFlag, val clientIntel: Short, val topologyId: Int,
                   val decoder: AbstractVersionedDecoder) extends RequestHeader(op) {
   override def toString = {
      new StringBuilder().append("HotRodHeader").append("{")
         .append("op=").append(op)
         .append(", version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", cacheName=").append(cacheName)
         .append(", flag=").append(flag)
         .append(", clientIntelligence=").append(clientIntel)
         .append(", topologyId=").append(topologyId)
         .append("}").toString()
   }
}

class CacheNotFoundException(msg: String, override val version: Byte, override val messageId: Long)
        extends RequestParsingException(msg, version, messageId)

class HotRodException(val response: ErrorResponse, cause: Throwable) extends Exception(cause)
