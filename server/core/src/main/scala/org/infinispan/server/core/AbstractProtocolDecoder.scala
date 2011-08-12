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
package org.infinispan.server.core

import org.infinispan.Cache
import Operation._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import transport._
import java.io.StreamCorruptedException
import transport.ExtendedChannelBuffer._
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import DecoderState._
import org.infinispan.util.ClusterIdGenerator
import logging.Log

/**
 * Common abstract decoder for Memcached and Hot Rod protocols.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractProtocolDecoder[K, V <: CacheValue](transport: NettyTransport)
        extends ReplayingDecoder[DecoderState](DECODE_HEADER, true) with Log {
   import AbstractProtocolDecoder._

   type SuitableParameters <: RequestParameters
   type SuitableHeader <: RequestHeader

   var versionGenerator: ClusterIdGenerator = _

   private val isTrace = isTraceEnabled

   protected var header: SuitableHeader = null.asInstanceOf[SuitableHeader]
   protected var params: SuitableParameters = null.asInstanceOf[SuitableParameters]
   protected var key: K = null.asInstanceOf[K]
   protected var rawValue: Array[Byte] = null.asInstanceOf[Array[Byte]]
   protected var cache: Cache[K, V] = null

   override def decode(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      val ch = ctx.getChannel
      try {
         if (isTraceEnabled) // To aid debugging
            trace("Decode using instance @%x", System.identityHashCode(this))
         state match {
            case DECODE_HEADER => decodeHeader(ch, buffer, state)
            case DECODE_KEY => decodeKey(ch, buffer, state)
            case DECODE_PARAMETERS => decodeParameters(ch, buffer, state)
            case DECODE_VALUE => decodeValue(ch, buffer, state)
         }
      } catch {
         case e: Exception => {
            val (serverException, isClientError) = createServerException(e, buffer)
            // If decode returns an exception, decode won't be called again so,
            // we need to fire the exception explicitly so that requests can
            // carry on being processed on same connection after a client error
            if (isClientError) {
               Channels.fireExceptionCaught(ch, serverException)
               null
            } else {
               throw serverException
            }
         }
         case t: Throwable => throw t
      }
   }

   private def decodeHeader(ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      val (optHeader, endOfOp) = readHeader(buffer)
      if (optHeader == None) return null // Something went wrong reading the header, so get more bytes
      header = optHeader.get
      cache = getCache
      if (endOfOp) {
         header.op match {
            case StatsRequest => writeResponse(ch, createStatsResponse)
            case _ => customDecodeHeader(ch, buffer)
         }
      } else {
         checkpointTo(DECODE_KEY)
      }
   }

   private def decodeKey(ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      header.op match {
         case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest | RemoveRequest => {
            val (k, endOfOp) = readKey(buffer)
            key = k
            if (endOfOp) {
               // If it's the end of the operation, it can only be a remove
               writeResponse(ch, remove)
            } else {
               checkpointTo(DECODE_PARAMETERS)
            }
         }
         case GetRequest | GetWithVersionRequest => writeResponse(ch, get(buffer))
         case _ => customDecodeKey(ch, buffer)
      }
   }

   private def decodeParameters(ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      val endOfOp = readParameters(ch, buffer)
      if (!endOfOp && params.valueLength > 0) {
         // Create value holder and checkpoint only if there's more to read
         rawValue = new Array[Byte](params.valueLength)
         checkpointTo(DECODE_VALUE)
      } else if (params.valueLength == 0){
         rawValue = Array.empty
         decodeValue(ch, buffer, state)
      } else {
         decodeValue(ch, buffer, state)
      }
   }

   private def decodeValue(ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      val ret = header.op match {
         case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest  => {
            readValue(buffer)
            header.op match {
               case PutRequest => put
               case PutIfAbsentRequest => putIfAbsent
               case ReplaceRequest => replace
               case ReplaceIfUnmodifiedRequest => replaceIfUmodified
            }
         }
         case RemoveRequest => remove
         case _ => customDecodeValue(ch, buffer)
      }
      writeResponse(ch, ret)
   }

   override def decodeLast(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = null // no-op

   protected def writeResponse(ch: Channel, response: AnyRef): AnyRef = {
      try {
         if (response != null) {
            if (isTrace) trace("Write response %s", response)
            response match {
               // We only expect Lists of ChannelBuffer instances, so don't worry about type erasure
               case l: List[ChannelBuffer] => l.foreach(ch.write(_))
               case a: Array[Byte] => ch.write(wrappedBuffer(a))
               case sb: StringBuilder => ch.write(wrappedBuffer(sb.toString.getBytes))
               case s: String => ch.write(wrappedBuffer(s.getBytes))
               case _ => ch.write(response)
            }
         }
         null
      } finally {
         resetParams
      }
   }

   private def resetParams: AnyRef = {
      checkpointTo(DECODE_HEADER)
      // Reset parameters to avoid leaking previous params
      // into a request that has no params
      params = null.asInstanceOf[SuitableParameters]
      null
   }

   private def put: AnyRef = {
      val v = createValue(generateVersion(cache))
      // Get an optimised cache in case we can make the operation more efficient
      val prev = getOptimizedCache(cache).put(key, v,
         toMillis(params.lifespan), DefaultTimeUnit,
         toMillis(params.maxIdle), DefaultTimeUnit)
      createSuccessResponse(prev)
   }

   protected def getOptimizedCache(c: Cache[K, V]): Cache[K, V] = c

   private def putIfAbsent: AnyRef = {
      var prev = cache.get(key)
      if (prev == null) { // Generate new version only if key not present
         val v = createValue(generateVersion(cache))
         prev = cache.putIfAbsent(key, v,
            toMillis(params.lifespan), DefaultTimeUnit,
            toMillis(params.maxIdle), DefaultTimeUnit)
      }
      if (prev == null)
         createSuccessResponse(prev)
      else
         createNotExecutedResponse(prev)
   }

   private def replace: AnyRef = {
      var prev = cache.get(key)
      if (prev != null) { // Generate new version only if key present
         val v = createValue(generateVersion(cache))
         prev = cache.replace(key, v,
            toMillis(params.lifespan), DefaultTimeUnit,
            toMillis(params.maxIdle), DefaultTimeUnit)
      }
      if (prev != null)
         createSuccessResponse(prev)
      else
         createNotExecutedResponse(prev)
   }

   private def replaceIfUmodified: AnyRef = {
      val prev = cache.get(key)
      if (prev != null) {
         if (prev.version == params.streamVersion) {
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            val v = createValue(generateVersion(cache))
            val replaced = cache.replace(key, prev, v,
                  toMillis(params.lifespan), DefaultTimeUnit,
                  toMillis(params.maxIdle), DefaultTimeUnit)
            if (replaced)
               createSuccessResponse(prev)
            else
               createNotExecutedResponse(prev)
         } else {
            createNotExecutedResponse(prev)
         }            
      } else createNotExistResponse
   }

   private def remove: AnyRef = {
      val prev = cache.remove(key)
      if (prev != null)
         createSuccessResponse(prev)
      else
         createNotExistResponse
   }

   protected def get(buffer: ChannelBuffer): AnyRef =
      createGetResponse(key, cache.get(readKey(buffer)._1))

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      val ch = ctx.getChannel
      val errorResponse = createErrorResponse(e.getCause)
      if (errorResponse != null) {
         errorResponse match {
            case a: Array[Byte] => ch.write(wrappedBuffer(a))
            case sb: StringBuilder => ch.write(wrappedBuffer(sb.toString.getBytes))
            case null => // ignore
            case _ => ch.write(errorResponse)
         }
      }
      // After writing back an error, reset params and revert to initial state
      resetParams
   }

   override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      transport.acceptedChannels.add(e.getChannel)
      super.channelOpen(ctx, e)
   }

   def checkpointTo(state: DecoderState): AnyRef = {
      checkpoint(state)
      null // For netty's decoder that mandates a return
   }

   protected def readHeader(b: ChannelBuffer): (Option[SuitableHeader], Boolean)

   protected def getCache: Cache[K, V]

   /**
    * Returns the key read along with a boolean indicating whether the
    * end of the operation was found or not. This allows client to
    * differentiate between extra parameters or pipelined sequence of
    * operations.
    */
   protected def readKey(b: ChannelBuffer): (K, Boolean)

   protected def readParameters(ch: Channel, b: ChannelBuffer): Boolean

   protected def readValue(b: ChannelBuffer)

   protected def createValue(nextVersion: Long): V

   protected def createSuccessResponse(prev: V): AnyRef

   protected def createNotExecutedResponse(prev: V): AnyRef

   protected def createNotExistResponse: AnyRef

   protected def createGetResponse(k: K, v: V): AnyRef

   protected def createMultiGetResponse(pairs: Map[K, V]): AnyRef
   
   protected def createErrorResponse(t: Throwable): AnyRef

   protected def createStatsResponse: AnyRef

   protected def customDecodeHeader(ch: Channel, buffer: ChannelBuffer): AnyRef

   protected def customDecodeKey(ch: Channel, buffer: ChannelBuffer): AnyRef

   protected def customDecodeValue(ch: Channel, buffer: ChannelBuffer): AnyRef

   protected def createServerException(e: Exception, b: ChannelBuffer): (Exception, Boolean)

   protected def generateVersion(cache: Cache[K, V]): Long = {
      val rpcManager = cache.getAdvancedCache.getRpcManager
      versionGenerator.newVersion(rpcManager != null)
   }

   /**
    * Transforms lifespan pass as seconds into milliseconds
    * following this rule:
    *
    * If lifespan is bigger than number of seconds in 30 days,
    * then it is considered unix time. After converting it to
    * milliseconds, we substract the current time in and the
    * result is returned.
    *
    * Otherwise it's just considered number of seconds from
    * now and it's returned in milliseconds unit.
    */
   protected def toMillis(lifespan: Int): Long = {
      if (lifespan > SecondsInAMonth) {
         val unixTimeExpiry = TimeUnit.SECONDS.toMillis(lifespan) - System.currentTimeMillis
         if (unixTimeExpiry < 0) 0 else unixTimeExpiry
      } else {
         TimeUnit.SECONDS.toMillis(lifespan)
      }
   }

   override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
      transport.updateTotalBytesWritten(e)
      ctx.sendUpstream(e)
   }

   override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
      transport.updateTotalBytesRead(e)
      super.messageReceived(ctx, e)
   }
}

object AbstractProtocolDecoder extends Log {
   private val SecondsInAMonth = 60 * 60 * 24 * 30
   private val DefaultTimeUnit = TimeUnit.MILLISECONDS
}

class RequestHeader(val op: Enumeration#Value) {
   override def toString = {
      new StringBuilder().append("RequestHeader").append("{")
         .append("op=").append(op)
         .append("}").toString
   }
}

class RequestParameters(val valueLength: Int, val lifespan: Int, val maxIdle: Int, val streamVersion: Long) {
   override def toString = {
      new StringBuilder().append("RequestParameters").append("{")
         .append("valueLength=").append(valueLength)
         .append(", lifespan=").append(lifespan)
         .append(", maxIdle=").append(maxIdle)
         .append(", streamVersion=").append(streamVersion)
         .append("}").toString
   }
}

class UnknownOperationException(reason: String) extends StreamCorruptedException(reason)
