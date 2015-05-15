package org.infinispan.server.hotrod

import java.io.{IOException, StreamCorruptedException}
import java.lang.StringBuilder
import java.security.PrivilegedExceptionAction
import javax.security.auth.Subject
import javax.security.sasl.SaslServer

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.handler.codec.ReplayingDecoder
import io.netty.util.CharsetUtil
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.security.Security
import HotRodDecoderState._
import org.infinispan.server.core.Operation._
import org.infinispan.server.core._
import org.infinispan.server.core.logging.Log
import org.infinispan.server.core.security.AuthorizingCallbackHandler
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.core.transport._

/**
 * Top level Hot Rod decoder that after figuring out the version, delegates the rest of the reading to the
 * corresponding versioned decoder.
 *
 * @author Galder Zamarreño
 * @author gustavonalle
 * @since 4.1
 */
class HotRodDecoder(cacheManager: EmbeddedCacheManager, val transport: NettyTransport, server: HotRodServer)
extends ReplayingDecoder[HotRodDecoderState](DECODE_HEADER) with StatsChannelHandler with ServerConstants with Constants with Log {

   val secure = server.getConfiguration.authentication().enabled()

   private val decodeCtx = new CacheDecodeContext(server)

   var saslServer: SaslServer = null
   var callbackHandler: AuthorizingCallbackHandler = null
   var subject: Subject = ANONYMOUS

   def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: java.util.List[AnyRef]): Unit = {
      try {
         if (decodeCtx.isTrace) trace("Decode using instance @%x", System.identityHashCode(this))
         wrapSecurity {
            state match {
               case DECODE_HEADER => decodeHeader(ctx, in, state, out)
               case DECODE_KEY => decodeKey(ctx, in, state)
               case DECODE_PARAMETERS => decodeParameters(ctx, in, state)
               case DECODE_VALUE => decodeValue(ctx, in, state)
            }
         }
      } catch {
         case e: Exception =>
            val (serverException, isClientError) = decodeCtx.createServerException(e, in)
            // If decode returns an exception, decode won't be called again so,
            // we need to fire the exception explicitly so that requests can
            // carry on being processed on same connection after a client error
            if (isClientError) {
               ctx.pipeline.fireExceptionCaught(serverException)
            } else {
               throw serverException
            }
         case t: Throwable => throw t
      }
   }

   private def decodeHeader(ctx: ChannelHandlerContext, buffer: ByteBuf, state: HotRodDecoderState, out: java.util.List[AnyRef]): AnyRef = {
      decodeCtx.header = new HotRodHeader
      val endOfOp = readHeader(buffer, decodeCtx.header)
      if (endOfOp.isEmpty) {
         // Something went wrong reading the header, so get more bytes.
         // It can happen with Hot Rod if the header is completely corrupted
         return null
      }
      val ch = ctx.channel
      decodeCtx.obtainCache(cacheManager)
      val cacheConfiguration = server.getCacheConfiguration(decodeCtx.header.cacheName)
      decodeCtx.defaultLifespanTime = cacheConfiguration.expiration().lifespan()
      decodeCtx.defaultMaxIdleTime = cacheConfiguration.expiration().maxIdle()
      if (endOfOp.get) {
         val message = decodeCtx.header.op match {
            case StatsRequest => writeResponse(ch, createStatsResponse)
            case _ => customDecodeHeader(ctx, buffer)
         }
         message match {
            case pr: PartialResponse => pr.buffer.map(out.add(_))
            case _ => null
         }
         null
      } else {
         checkpointTo(DECODE_KEY)
      }
   }

   private def decodeKey(ctx: ChannelHandlerContext, buffer: ByteBuf, state: HotRodDecoderState): AnyRef = {
      val ch = ctx.channel
      decodeCtx.header.op match {
         // Get, put and remove are the most typical operations, so they're first
         case GetRequest => writeResponse(ch, decodeCtx.get(readKey(buffer)._1))
         case PutRequest => handleModification(ch, buffer)
         case RemoveRequest => handleModification(ch, buffer)
         case GetWithVersionRequest => writeResponse(ch, decodeCtx.get(readKey(buffer)._1))
         case PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest =>
            handleModification(ch, buffer)
         case _ => customDecodeKey(ctx, buffer)
      }
   }

   private def decodeParameters(ctx: ChannelHandlerContext, buffer: ByteBuf, state: HotRodDecoderState): AnyRef = {
      val ch = ctx.channel
      val endOfOp = readParameters(ch, buffer)
      checkpointTo(DECODE_VALUE)
      if (!endOfOp && decodeCtx.params.valueLength > 0) {
         // Create value holder only if there's more to read
         decodeCtx.rawValue = new Bytes(decodeCtx.params.valueLength)
         null
      } else if (decodeCtx.params.valueLength == 0) {
         decodeCtx.rawValue = Array.empty
         decodeValue(ctx, buffer, state)
      } else {
         decodeValue(ctx, buffer, state)
      }
   }

   private def decodeValue(ctx: ChannelHandlerContext, buffer: ByteBuf, state: HotRodDecoderState): AnyRef = {
      val ch = ctx.channel
      val ret = decodeCtx.header.op match {
         case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest =>
            buffer.readBytes(decodeCtx.rawValue)
            decodeCtx.header.op match {
               case PutRequest => decodeCtx.put
               case PutIfAbsentRequest => decodeCtx.putIfAbsent
               case ReplaceRequest => decodeCtx.replace
               case ReplaceIfUnmodifiedRequest => decodeCtx.replaceIfUnmodified
            }
         case RemoveRequest => decodeCtx.remove
         case _ => customDecodeValue(ctx, buffer)
      }
      writeResponse(ch, ret)
   }

   def readHeader(buffer: ByteBuf, header: HotRodHeader): Option[Boolean] = {
      try {
         val magic = buffer.readUnsignedByte
         if (magic != MAGIC_REQ) {
            if (!decodeCtx.isError) {
               throw new InvalidMagicIdException("Error reading magic byte or message id: " + magic)
            } else {
               trace("Error happened previously, ignoring %d byte until we find the magic number again", magic)
               return None // Keep trying to read until we find magic
            }
         }
      } catch {
         case e: Exception =>
            decodeCtx.isError = true
            throw e
      }

      val messageId = readUnsignedLong(buffer)
      val version = buffer.readUnsignedByte.toByte

      try {
         val decoder = version match {
            case VERSION_10 | VERSION_11 | VERSION_12 | VERSION_13 => Decoder10
            case VERSION_20 | VERSION_21 | VERSION_22 | VERSION_23 => Decoder2x
            case _ => throw new UnknownVersionException("Unknown version:" + version, version, messageId)
         }
         val endOfOp = decoder.readHeader(buffer, version, messageId, header)
         decodeCtx.decoder = decoder
         if (decodeCtx.isTrace) trace("Decoded header %s", header)
         decodeCtx.isError = false
         Some(endOfOp)
      } catch {
         case e: HotRodUnknownOperationException =>
            decodeCtx.isError = true
            throw e
         case e: Exception =>
            decodeCtx.isError = true
            throw new RequestParsingException("Unable to parse header", version, messageId, e)
      }
   }

   def readKey(b: ByteBuf): (Bytes, Boolean) = decodeCtx.decoder.readKey(decodeCtx.header, b)

   def readParameters(ch: Channel, b: ByteBuf): Boolean = {
      val (parameters, endOfOp) = decodeCtx.decoder.readParameters(decodeCtx.header, b)
      decodeCtx.params = parameters
      endOfOp
   }

   protected def customDecodeHeader(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef =
      writeResponse(ctx.channel, decodeCtx.decoder.customReadHeader(decodeCtx.header,  buffer, decodeCtx.cache, server, ctx))

   protected def customDecodeKey(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef =
      writeResponse(ctx.channel, decodeCtx.decoder.customReadKey(this, decodeCtx.header, buffer, decodeCtx.cache, server, ctx.channel))

   protected def customDecodeValue(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef =
      writeResponse(ctx.channel, decodeCtx.decoder.customReadValue(this, decodeCtx.header, decodeCtx, buffer, decodeCtx.cache))

   def createStatsResponse: Response =
      decodeCtx.decoder.createStatsResponse(decodeCtx.header, decodeCtx.cache.getStats, transport)

   private def wrapSecurity(block: => Unit) = {
      if (secure) Security.doAs(subject, new PrivilegedExceptionAction[Unit] {
         override def run(): Unit = block
      })
      else block
   }

   def handleModification(ch: Channel, buf: ByteBuf): AnyRef = {
      val (k, endOfOp) = readKey(buf)
      decodeCtx.key = k
      if (endOfOp) {
         // If it's the end of the operation, it can only be a remove
         writeResponse(ch, decodeCtx.remove)
      } else {
         checkpointTo(DECODE_PARAMETERS)
      }
   }

   protected def writeResponse(ch: Channel, response: AnyRef): AnyRef = {
       if (response != null) {
         try {
            if (decodeCtx.isTrace) trace("Write response %s", response)
            response match {
               // We only expect Lists of ChannelBuffer instances, so don't worry about type erasure
               case l: Array[ByteBuf] =>
                  l.foreach(ch.write(_))
                  ch.flush
               case a: Bytes => ch.writeAndFlush(wrappedBuffer(a))
               case cs: CharSequence => ch.writeAndFlush(Unpooled.copiedBuffer(cs, CharsetUtil.UTF_8))
               case pr: PartialResponse => return pr
               case _ => ch.writeAndFlush(response)
            }
         } finally {
           resetParams
         }
      }
         null
   }

   private def resetParams() = {
      checkpointTo(DECODE_HEADER)
      // Reset parameters to avoid leaking previous params
      // into a request that has no params
      decodeCtx.resetParams()
      null
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
      decodeCtx.exceptionCaught(ctx, cause)(resetParams())
   }

   def checkpointTo(state: HotRodDecoderState): AnyRef = {
      checkpoint(state)
      null // For netty's decoder that mandates a return
   }

   override def checkpoint() = {
     super.checkpoint
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

class HotRodHeader {
   var op: Enumeration#Value = _
   var version: Byte = _
   var messageId: Long = _
   var cacheName: String = _
   var flag: Int = _
   var clientIntel: Short = _
   var topologyId: Int = _

   override def toString = {
      new StringBuilder().append("HotRodHeader").append("{")
      .append("op=").append(op)
      .append(", version=").append(version)
      .append(", messageId=").append(messageId)
      .append(", cacheName=").append(cacheName)
      .append(", flag=").append(flag)
      .append(", clientIntelligence=").append(clientIntel)
      .append(", topologyId=").append(topologyId)
      .append("}").toString
   }
}

class CacheNotFoundException(msg: String, override val version: Byte, override val messageId: Long)
extends RequestParsingException(msg, version, messageId)

class HotRodException(val response: ErrorResponse, cause: Throwable) extends Exception(cause)

class UnknownOperationException(reason: String) extends StreamCorruptedException(reason)

class PartialResponse(val buffer: Option[ByteBuf])
