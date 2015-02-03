package org.infinispan.server.memcached

import java.io.{ByteArrayOutputStream, EOFException, IOException, StreamCorruptedException}
import java.lang.StringBuilder
import java.nio.channels.ClosedChannelException
import java.util
import java.util.concurrent.TimeUnit.{MILLISECONDS => MILLIS}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import io.netty.handler.codec.ReplayingDecoder
import io.netty.buffer._
import io.netty.channel._
import io.netty.util.CharsetUtil
import org.infinispan._
import org.infinispan.commons.CacheException
import org.infinispan.configuration.cache.Configuration
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.versioning.{EntryVersion, NumericVersion, NumericVersionGenerator, VersionGenerator}
import org.infinispan.context.Flag
import org.infinispan.factories.ComponentRegistry
import org.infinispan.metadata.Metadata
import org.infinispan.remoting.rpc.RpcManager
import org.infinispan.server.core.Operation._
import org.infinispan.server.core._
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.core.transport.{NettyTransport, StatsChannelHandler}
import org.infinispan.server.memcached.MemcachedDecoderState._
import org.infinispan.server.memcached.MemcachedOperation._
import org.infinispan.server.memcached.TextProtocolUtil._
import org.infinispan.server.memcached.logging.Log

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

/**
 * A Memcached protocol specific decoder
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class MemcachedDecoder(memcachedCache: AdvancedCache[String, Array[Byte]], scheduler: ScheduledExecutorService, val transport: NettyTransport)
extends ReplayingDecoder[MemcachedDecoderState](DECODE_HEADER) with StatsChannelHandler with ServerConstants {

   val SecondsInAMonth = 60 * 60 * 24 * 30
   val DefaultTimeUnit = TimeUnit.MILLISECONDS
   var defaultLifespanTime: Long = _
   var defaultMaxIdleTime: Long = _

   protected var key: String = null
   protected var rawValue: Array[Byte] = null
   protected var cacheConfiguration: Configuration = null

   var cache =
      if (memcachedCache.getCacheConfiguration.compatibility().enabled())
         memcachedCache.getAdvancedCache.withFlags(Flag.OPERATION_MEMCACHED)
      else memcachedCache

   import org.infinispan.server.memcached.RequestResolver._

   protected var params: MemcachedParameters = null
   private lazy val isStatsEnabled = cache.getCacheConfiguration.jmxStatistics().enabled()
   private final val incrMisses = new AtomicLong(0)
   private final val incrHits = new AtomicLong(0)
   private final val decrMisses = new AtomicLong(0)
   private final val decrHits = new AtomicLong(0)
   private final val replaceIfUnmodifiedMisses = new AtomicLong(0)
   private final val replaceIfUnmodifiedHits = new AtomicLong(0)
   private final val replaceIfUnmodifiedBadval = new AtomicLong(0)
   private val isTrace = isTraceEnabled
   private val byteBuffer = new ByteArrayOutputStream()
   protected var header: RequestHeader = _

   override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
      try {
         decodeDispatch(ctx, in, out)
      } finally {
         // reset in all cases
         byteBuffer.reset()
      }
   }

   protected def replace: AnyRef = {
      // Avoid listener notification for a simple optimization
      // on whether a new version should be calculated or not.
      var prev = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).get(key)
      if (prev != null) {
         // Generate new version only if key present
         prev = cache.replace(key, createValue(), buildMetadata())
      }
      if (prev != null)
         createSuccessResponse(prev)
      else
         createNotExecutedResponse(prev)
   }

   protected def replaceIfUnmodified: AnyRef = {
      val entry = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntry(key)
      if (entry != null) {
         val prev = entry.getValue
         val streamVersion = new NumericVersion(params.streamVersion)
         if (entry.getMetadata.version() == streamVersion) {
            val v = createValue()
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            val replaced = cache.replace(key, prev, v, buildMetadata())
            if (replaced)
               createSuccessResponse(prev)
            else
               createNotExecutedResponse(prev)
         } else {
            createNotExecutedResponse(prev)
         }
      } else createNotExistResponse
   }

   private def decodeDispatch(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
      try {
         if (isTrace) // To aid debugging
            trace("Decode using instance @%x", System.identityHashCode(this))
         state match {
            case DECODE_HEADER => decodeHeader(ctx, in, state, out)
            case DECODE_KEY => decodeKey(ctx, in, state)
            case DECODE_PARAMETERS => decodeParameters(ctx, in, state)
            case DECODE_VALUE => decodeValue(ctx, in, state)
         }
      } catch {
         case e: Exception =>
            val (serverException, isClientError) = createServerException(e, in)
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

   def decodeHeader(ctx: ChannelHandlerContext, buffer: ByteBuf, state: MemcachedDecoderState, out: util.List[AnyRef]): AnyRef = {
      header = createHeader
      val endOfOp = readHeader(buffer, header)
      if (endOfOp == None) {
         // Something went wrong reading the header, so get more bytes.
         // It can happen with Hot Rod if the header is completely corrupted
         return null
      }
      val ch = ctx.channel
      cache = getCache.getAdvancedCache
      cacheConfiguration = getCacheConfiguration
      defaultLifespanTime = cacheConfiguration.expiration().lifespan()
      defaultMaxIdleTime = cacheConfiguration.expiration().maxIdle()
      if (endOfOp.get) {
         val message = header.op match {
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

   def decodeKey(ctx: ChannelHandlerContext, buffer: ByteBuf, state: MemcachedDecoderState): AnyRef = {
      val ch = ctx.channel
      header.op match {
         // Get, put and remove are the most typical operations, so they're first
         case GetRequest => writeResponse(ch, get(buffer))
         case PutRequest => handleModification(ch, buffer)
         case RemoveRequest => handleModification(ch, buffer)
         case GetWithVersionRequest => writeResponse(ch, get(buffer))
         case PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest =>
            handleModification(ch, buffer)
         case _ => customDecodeKey(ctx, buffer)
      }
   }

   def decodeParameters(ctx: ChannelHandlerContext, buffer: ByteBuf, state: MemcachedDecoderState): AnyRef = {
      val ch = ctx.channel
      val endOfOp = readParameters(ch, buffer)
      if (!endOfOp && params.valueLength > 0) {
         // Create value holder and checkpoint only if there's more to read
         rawValue = new Array[Byte](params.valueLength)
         checkpointTo(DECODE_VALUE)
      } else if (params.valueLength == 0) {
         rawValue = Array.empty
         decodeValue(ctx, buffer, state)
      } else {
         decodeValue(ctx, buffer, state)
      }
   }

   def decodeValue(ctx: ChannelHandlerContext, buffer: ByteBuf, state: MemcachedDecoderState): AnyRef = {
      val ch = ctx.channel
      val ret = header.op match {
         case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest =>
            readValue(buffer)
            header.op match {
               case PutRequest => put
               case PutIfAbsentRequest => putIfAbsent
               case ReplaceRequest => replace
               case ReplaceIfUnmodifiedRequest => replaceIfUnmodified
            }
         case RemoveRequest => remove
         case _ => customDecodeValue(ctx, buffer)
      }
      writeResponse(ch, ret)
   }


   private def putIfAbsent: AnyRef = {
      var prev = cache.get(key)
      if (prev == null) {
         // Generate new version only if key not present
         prev = cache.putIfAbsent(key, createValue(), buildMetadata())
      }
      if (prev == null)
         createSuccessResponse(prev)
      else
         createNotExecutedResponse(prev)
   }

   private def put: AnyRef = {
      // Get an optimised cache in case we can make the operation more efficient
      val prev = cache.put(key, createValue(), buildMetadata())
      createSuccessResponse(prev)
   }


   def createHeader: RequestHeader = new RequestHeader

   def readHeader(buffer: ByteBuf, header: RequestHeader): Option[Boolean] = {
      var endOfOp = readElement(buffer, byteBuffer)
      val streamOp = extractString(byteBuffer)
      val op = toRequest(streamOp, endOfOp, buffer)
      if (op == StatsRequest && !endOfOp) {
         val line = readDiscardedLine(buffer).trim
         if (!line.isEmpty)
            throw new StreamCorruptedException("Stats command does not accept arguments: " + line)
         else
            endOfOp = true
      }
      if (op == VerbosityRequest) {
         if (!endOfOp)
            skipLine(buffer) // Read rest of line to clear the operation
         throw new StreamCorruptedException("Memcached 'verbosity' command is unsupported")
      }

      header.op = op
      Some(endOfOp)
   }

   def readKey(b: ByteBuf): (String, Boolean) = {
      val endOfOp = readElement(b, byteBuffer)
      val k = extractString(byteBuffer)
      checkKeyLength(k, endOfOp, b)
      (k, endOfOp)
   }

   private def readKeys(b: ByteBuf): Seq[String] = readSplitLine(b)

   override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
      val ch = ctx.channel
      // Log it just in case the channel is closed or similar
      debug(cause, "Exception caught")
      if (!cause.isInstanceOf[IOException]) {
         val errorResponse = createErrorResponse(cause)
         if (errorResponse != null) {
            errorResponse match {
               case a: Array[Byte] => ch.writeAndFlush(wrappedBuffer(a))
               case cs: CharSequence => ch.writeAndFlush(Unpooled.copiedBuffer(cs, CharsetUtil.UTF_8))
               case null => // ignore
               case _ => ch.writeAndFlush(errorResponse)
            }
         }
      }
      // After writing back an error, reset params and revert to initial state
      resetParams
   }

   protected def get(buffer: ByteBuf): AnyRef = {
      val keys = readKeys(buffer)
      if (keys.length > 1) {
         val map = new mutable.HashMap[String, CacheEntry[String, Array[Byte]]]()
         for (k <- keys) {
            val entry = cache.getCacheEntry(checkKeyLength(k, endOfOp = true, buffer))
            if (entry != null)
               map += (k -> entry)
         }
         createMultiGetResponse(new immutable.HashMap ++ map)
      } else {
         val key = checkKeyLength(keys(0), endOfOp = true, buffer)
         val entry = cache.getCacheEntry(key)
         createGetResponse(key, entry)
      }
   }

   private def checkKeyLength(k: String, endOfOp: Boolean, b: ByteBuf): String = {
      if (k.length > 250) {
         if (!endOfOp) skipLine(b) // Clear the rest of line
         throw new StreamCorruptedException("Key length over the 250 character limit")
      } else k
   }

   def readParameters(ch: Channel, b: ByteBuf): Boolean = {
      val args = readSplitLine(b)
      var endOfOp = false
      params =
      if (args.nonEmpty) {
         if (isTrace) trace("Operation parameters: %s", args)
         try {
            header.op match {
               case PutRequest => readStorageParameters(args, b)
               case RemoveRequest => readRemoveParameters(args)
               case IncrementRequest | DecrementRequest =>
                  endOfOp = true
                  readIncrDecrParameters(args)
               case FlushAllRequest => readFlushAllParameters(args)
               case _ => readStorageParameters(args, b)
            }
         } catch {
            case _: ArrayIndexOutOfBoundsException => throw new IOException("Missing content in command line " + args)
         }
      } else {
         null // For example when delete <key> is sent without any further parameters, or flush_all without delay
      }
      endOfOp
   }

   private def readRemoveParameters(args: Seq[String]): MemcachedParameters = {
      val delayedDeleteTime = parseDelayedDeleteTime(args)
      val noReply = if (delayedDeleteTime == -1) parseNoReply(0, args) else false
      new MemcachedParameters(-1, -1, -1, -1, noReply, 0, "", 0)
   }

   private def readIncrDecrParameters(args: Seq[String]): MemcachedParameters = {
      val delta = args(0)
      new MemcachedParameters(-1, -1, -1, -1, parseNoReply(1, args), 0, delta, 0)
   }

   private def readFlushAllParameters(args: Seq[String]): MemcachedParameters = {
      var noReplyFound = false
      val flushDelay =
         try {
            friendlyMaxIntCheck(args(0), "Flush delay")
         } catch {
            case n: NumberFormatException =>
               if (n.getMessage.contains("noreply")) {
                  noReplyFound = true
                  0
               } else throw n
         }
      val noReply = if (!noReplyFound) parseNoReply(1, args) else true
      new MemcachedParameters(-1, -1, -1, -1, noReply, 0, "", flushDelay)
   }

   private def readStorageParameters(args: Seq[String], b: ByteBuf): MemcachedParameters = {
      var index = 0
      val flags = getFlags(args(index))
      if (flags < 0) throw new StreamCorruptedException("Flags cannot be negative: " + flags)
      index += 1
      val lifespan = {
         val streamLifespan = getLifespan(args(index))
         if (streamLifespan <= 0) -1 else streamLifespan
      }
      index += 1
      val length = getLength(args(index))
      if (length < 0) throw new StreamCorruptedException("Negative bytes length provided: " + length)
      val streamVersion = header.op match {
         case ReplaceIfUnmodifiedRequest =>
            index += 1
            getVersion(args(index))
         case _ => -1
      }
      index += 1
      val noReply = parseNoReply(index, args)
      new MemcachedParameters(length, lifespan, -1, streamVersion, noReply, flags, "", 0)
   }

   protected def generateVersion(cache: Cache[String, Array[Byte]]): EntryVersion = {
      val registry = getCacheRegistry
      val cacheVersionGenerator = registry.getComponent(classOf[VersionGenerator])
      if (cacheVersionGenerator == null) {
         // It could be null, for example when not running in compatibility mode.
         // The reason for that is that if no other component depends on the
         // version generator, the factory does not get invoked.
         val newVersionGenerator = new NumericVersionGenerator()
         .clustered(registry.getComponent(classOf[RpcManager]) != null)
         registry.registerComponent(newVersionGenerator, classOf[VersionGenerator])
         newVersionGenerator.generateNew()
      } else {
         cacheVersionGenerator.generateNew()
      }
   }


   protected def readValue(b: ByteBuf) {
      b.readBytes(rawValue)
      skipLine(b) // read the rest of line to clear CRLF after value Byte[]
   }

   override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
      val readable = msg.asInstanceOf[ByteBuf].readableBytes()
      ctx.write(msg, promise.addListener(new ChannelFutureListener {
         def operationComplete(future: ChannelFuture): Unit = {
            if (future.isSuccess) {
               transport.updateTotalBytesWritten(readable)
            }
         }
      }))
   }

   def createValue(): Array[Byte] = rawValue

   private def getFlags(flags: String): Long = {
      if (flags == null) throw new EOFException("No flags passed")
      try {
         numericLimitCheck(flags, 4294967295L, "Flags")
      } catch {
         case n: NumberFormatException => numericLimitCheck(flags, 4294967295L, "Flags", n)
      }
   }

   private def getLifespan(lifespan: String): Int = {
      if (lifespan == null) throw new EOFException("No expiry passed")
      friendlyMaxIntCheck(lifespan, "Lifespan")
   }

   private def getLength(length: String): Int = {
      if (length == null) throw new EOFException("No bytes passed")
      friendlyMaxIntCheck(length, "The number of bytes")
   }

   private def getVersion(version: String): Long = {
      if (version == null) throw new EOFException("No cas passed")
      version.toLong
   }

   private def parseNoReply(expectedIndex: Int, args: Seq[String]): Boolean = {
      if (args.length > expectedIndex) {
         if ("noreply" == args(expectedIndex))
            true
         else
            throw new StreamCorruptedException("Unable to parse noreply optional argument")
      }
      else false
   }

   private def parseDelayedDeleteTime(args: Seq[String]): Int = {
      if (args.length > 0) {
         try {
            args(0).toInt
         }
         catch {
            case e: NumberFormatException => -1 // Either unformatted number, or noreply found
         }
      }
      else 0
   }

   def getCache: Cache[String, Array[Byte]] = cache

   def getCacheConfiguration: Configuration = cache.getCacheConfiguration

   def getCacheRegistry: ComponentRegistry = cache.getComponentRegistry

   protected def customDecodeHeader(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef = {
      val ch = ctx.channel
      header.op match {
         case FlushAllRequest => flushAll(buffer, ch, isReadParams = false) // Without params
         case VersionRequest =>
            val ret = new StringBuilder().append("VERSION ").append(Version.getVersion).append(CRLF)
            writeResponse(ch, ret)
         case QuitRequest => closeChannel(ch)
      }
   }

   protected def customDecodeKey(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef = {
      val ch = ctx.channel
      header.op match {
         case AppendRequest | PrependRequest | IncrementRequest | DecrementRequest =>
            key = readKey(buffer)._1
            checkpointTo(DECODE_PARAMETERS)
         case FlushAllRequest => flushAll(buffer, ch, isReadParams = true) // With params
      }
   }

   protected def customDecodeValue(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef = {
      val ch = ctx.channel
      val op = header.op
      op match {
         case AppendRequest | PrependRequest =>
            readValue(buffer)
            val prev = cache.get(key)
            val ret =
               if (prev != null) {
                  val concatenated = header.op match {
                     case AppendRequest => concat(prev, rawValue)
                     case PrependRequest => concat(rawValue, prev)
                  }
                  val replaced = cache.replace(key, prev, concatenated, buildMetadata())
                  if (replaced)
                     if (!params.noReply) STORED else null
                  else // If there's a concurrent modification on this key, treat it as we couldn't replace it
                  if (!params.noReply) NOT_STORED else null
               } else {
                  if (!params.noReply) NOT_STORED else null
               }
            writeResponse(ch, ret)
         case IncrementRequest | DecrementRequest => incrDecr(ch)
      }
   }

   private def incrDecr(ch: Channel): AnyRef = {
      val prev = cache.get(key)
      val op = header.op
      val ret =
         if (prev != null) {
            val prevCounter = BigInt(new String(prev))
            val delta = validateDelta(params.delta)
            val newCounter =
               op match {
                  case IncrementRequest =>
                     val candidateCounter = prevCounter + delta
                     if (candidateCounter > MAX_UNSIGNED_LONG) 0 else candidateCounter
                  case DecrementRequest =>
                     val candidateCounter = prevCounter - delta
                     if (candidateCounter < 0) 0 else candidateCounter
               }

            val next = newCounter.toString.getBytes
            val replaced = cache.replace(key, prev, next, buildMetadata())
            if (replaced) {
               if (isStatsEnabled) if (op == IncrementRequest) incrHits.incrementAndGet() else decrHits.incrementAndGet
               if (!params.noReply) new String(next) + CRLF else null
            } else {
               // If there's a concurrent modification on this key, the spec does not say what to do, so treat it as exceptional
               throw new CacheException("Value modified since we retrieved from the cache, old value was " + prevCounter)
            }
         }
         else {
            if (isStatsEnabled) if (op == IncrementRequest) incrMisses.incrementAndGet() else decrMisses.incrementAndGet
            if (!params.noReply) NOT_FOUND else null
         }
      writeResponse(ch, ret)
   }

   private def flushAll(b: ByteBuf, ch: Channel, isReadParams: Boolean): AnyRef = {
      if (isReadParams) readParameters(ch, b)
      val flushFunction = (cache: AdvancedCache[String, Array[Byte]]) => cache.clear()
      val flushDelay = if (params == null) 0 else params.flushDelay
      if (flushDelay == 0)
         flushFunction(cache)
      else
         scheduler.schedule(new DelayedFlushAll(cache, flushFunction), toMillis(flushDelay), MILLIS)
      val ret = if (params == null || !params.noReply) OK else null
      writeResponse(ch, ret)
   }

   private def validateDelta(delta: String): BigInt = {
      val bigIntDelta = BigInt(delta)
      if (bigIntDelta > MAX_UNSIGNED_LONG)
         throw new StreamCorruptedException("Increment or decrement delta sent (" + delta + ") exceeds unsigned limit (" + MAX_UNSIGNED_LONG + ")")
      else if (bigIntDelta < MIN_UNSIGNED)
         throw new StreamCorruptedException("Increment or decrement delta cannot be negative: " + delta)
      bigIntDelta
   }

   def createSuccessResponse(prev: Array[Byte]): AnyRef = {
      if (isStatsEnabled) {
         header.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedHits.incrementAndGet
            case _ => // No-op
         }
      }
      if (params == null || !params.noReply) {
         header.op match {
            case RemoveRequest => DELETED
            case _ => STORED
         }
      } else null
   }

   def createNotExecutedResponse(prev: Array[Byte]): AnyRef = {
      if (isStatsEnabled) {
         header.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedBadval.incrementAndGet
            case _ => // No-op
         }
      }
      if (params == null || !params.noReply) {
         header.op match {
            case ReplaceIfUnmodifiedRequest => EXISTS
            case _ => NOT_STORED
         }
      } else null
   }

   def createNotExistResponse: AnyRef = {
      if (isStatsEnabled) {
         header.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedMisses.incrementAndGet
            case _ => // No-op
         }
      }
      if (params == null || !params.noReply)
         NOT_FOUND
      else
         null
   }

   def createGetResponse(k: String, entry: CacheEntry[String, Array[Byte]]): AnyRef = {
      if (entry != null) {
         header.op match {
            case GetRequest =>
               buildSingleGetResponse(k, entry)
            case GetWithVersionRequest =>
               buildSingleGetWithVersionResponse(k, entry)
         }
      }
      else
         END
   }

   private def buildSingleGetResponse(k: String, entry: CacheEntry[String, Array[Byte]]): ByteBuf = {
      val buf = buildGetHeaderBegin(k, entry, END_SIZE)
      writeGetHeaderData(entry.getValue, buf)
      writeGetHeaderEnd(buf)
   }

   def createMultiGetResponse(pairs: Map[String, CacheEntry[String, Array[Byte]]]): AnyRef = {
      val elements = new ListBuffer[ByteBuf]
      val op = header.op
      op match {
         case GetRequest | GetWithVersionRequest =>
            for ((k, entry) <- pairs)
               elements += buildGetResponse(op, k, entry)
            elements += wrappedBuffer(END)
      }
      elements.toArray
   }

   def handleModification(ch: Channel, buf: ByteBuf): AnyRef = {
      val (k, endOfOp) = readKey(buf)
      key = k
      if (endOfOp) {
         // If it's the end of the operation, it can only be a remove
         writeResponse(ch, remove)
      } else {
         checkpointTo(DECODE_PARAMETERS)
      }
   }

   def checkpointTo(state: MemcachedDecoderState): AnyRef = {
      checkpoint(state)
      null // For netty's decoder that mandates a return
   }

   private def resetParams: AnyRef = {
      checkpointTo(DECODE_HEADER)
      // Reset parameters to avoid leaking previous params
      // into a request that has no params
      params = null
      rawValue = null // Clear reference to value
      null
   }


   protected def remove: AnyRef = {
      val prev = cache.remove(key)
      if (prev != null)
         createSuccessResponse(prev)
      else
         createNotExistResponse
   }

   def createErrorResponse(t: Throwable): AnyRef = {
      val sb = new StringBuilder
      t match {
         case m: MemcachedException =>
            m.getCause match {
               case u: UnknownOperationException =>
                  logExceptionReported(u)
                  ERROR
               case c: ClosedChannelException =>
                  logExceptionReported(c)
                  null // no-op, only log
               case i: IOException =>
                  logAndCreateErrorMessage(sb, m)
               case n: NumberFormatException =>
                  logAndCreateErrorMessage(sb, m)
               case i: IllegalStateException =>
                  logAndCreateErrorMessage(sb, m)
               case _ => sb.append(m.getMessage).append(CRLF)
            }
         case c: ClosedChannelException =>
            logExceptionReported(c)
            null // no-op, only log
         case _ => sb.append(SERVER_ERROR).append(t.getMessage).append(CRLF)
      }
   }

   protected def buildMetadata(): Metadata = {
      val metadata = new MemcachedMetadataBuilder
      metadata.version(generateVersion(cache))
      metadata.flags(params.flags)
      if (params.lifespan > 0)
         metadata.lifespan(toMillis(params.lifespan))

      metadata.build()
   }

   private def logAndCreateErrorMessage(sb: StringBuilder, m: MemcachedException): StringBuilder = {
      logExceptionReported(m.getCause)
      sb.append(m.getMessage).append(CRLF)
   }

   protected def createServerException(e: Exception, b: ByteBuf): (MemcachedException, Boolean) = {
      e match {
         case i: IOException => (new MemcachedException(CLIENT_ERROR_BAD_FORMAT + i.getMessage, i), true)
         case n: NumberFormatException => (new MemcachedException(CLIENT_ERROR_BAD_FORMAT + n.getMessage, n), true)
         case _ => (new MemcachedException(SERVER_ERROR + e, e), false)
      }
   }

   private def closeChannel(ch: Channel): AnyRef = {
      ch.close
      null
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

   protected def writeResponse(ch: Channel, response: AnyRef): AnyRef = {
      try {
         if (response != null) {
            if (isTrace) trace("Write response %s", response)
            response match {
               // We only expect Lists of ChannelBuffer instances, so don't worry about type erasure
               case l: Array[ByteBuf] =>
                  l.foreach(ch.write(_))
                  ch.flush
               case a: Array[Byte] => ch.writeAndFlush(wrappedBuffer(a))
               case cs: CharSequence => ch.writeAndFlush(Unpooled.copiedBuffer(cs, CharsetUtil.UTF_8))
               case pr: PartialResponse => return pr
               case _ => ch.writeAndFlush(response)
            }
         }
         null
      } finally {
         resetParams
      }
   }

   def createStatsResponse: AnyRef = {
      val stats = cache.getAdvancedCache.getStats
      val sb = new StringBuilder
      Array[ByteBuf](
         buildStat("pid", 0, sb),
         buildStat("uptime", stats.getTimeSinceStart, sb),
         buildStat("uptime", stats.getTimeSinceStart, sb),
         buildStat("time", MILLIS.toSeconds(System.currentTimeMillis), sb),
         buildStat("version", cache.getVersion, sb),
         buildStat("pointer_size", 0, sb), // Unsupported
         buildStat("rusage_user", 0, sb), // Unsupported
         buildStat("rusage_system", 0, sb), // Unsupported
         buildStat("curr_items", stats.getCurrentNumberOfEntries, sb),
         buildStat("total_items", stats.getTotalNumberOfEntries, sb),
         buildStat("bytes", 0, sb), // Unsupported
         buildStat("curr_connections", 0, sb), // TODO: Through netty?
         buildStat("total_connections", 0, sb), // TODO: Through netty?
         buildStat("connection_structures", 0, sb), // Unsupported
         buildStat("cmd_get", stats.getRetrievals, sb),
         buildStat("cmd_set", stats.getStores, sb),
         buildStat("get_hits", stats.getHits, sb),
         buildStat("get_misses", stats.getMisses, sb),
         buildStat("delete_misses", stats.getRemoveMisses, sb),
         buildStat("delete_hits", stats.getRemoveHits, sb),
         buildStat("incr_misses", incrMisses, sb),
         buildStat("incr_hits", incrHits, sb),
         buildStat("decr_misses", decrMisses, sb),
         buildStat("decr_hits", decrHits, sb),
         buildStat("cas_misses", replaceIfUnmodifiedMisses, sb),
         buildStat("cas_hits", replaceIfUnmodifiedHits, sb),
         buildStat("cas_badval", replaceIfUnmodifiedBadval, sb),
         buildStat("auth_cmds", 0, sb), // Unsupported
         buildStat("auth_errors", 0, sb), // Unsupported
         //TODO: Evictions are measure by evict calls, but not by nodes are that are expired after the entry's lifespan has expired.
         buildStat("evictions", stats.getEvictions, sb),
         buildStat("bytes_read", transport.getTotalBytesRead, sb),
         buildStat("bytes_written", transport.getTotalBytesWritten, sb),
         buildStat("limit_maxbytes", 0, sb), // Unsupported
         buildStat("threads", 0, sb), // TODO: Through netty?
         buildStat("conn_yields", 0, sb), // Unsupported
         buildStat("reclaimed", 0, sb), // Unsupported
         wrappedBuffer(END)
      )
   }

   private def buildStat(stat: String, value: Any, sb: StringBuilder): ByteBuf = {
      sb.append("STAT").append(' ').append(stat).append(' ').append(value).append(CRLF)
      val buffer = wrappedBuffer(sb.toString.getBytes)
      sb.setLength(0)
      buffer
   }

   private def buildGetResponse(op: Enumeration#Value, k: String, entry: CacheEntry[String, Array[Byte]]): ByteBuf = {
      val buf = buildGetHeaderBegin(k, entry, 0)
      writeGetHeaderData(entry.getValue, buf)
   }

   private def buildGetHeaderBegin(k: String, entry: CacheEntry[String, Array[Byte]],
                                   extraSpace: Int): ByteBuf = {
      val data = entry.getValue
      val dataSize = Integer.valueOf(data.length).toString.getBytes
      val key = k.getBytes

      val flags = entry.getMetadata match {
         case meta: MemcachedMetadata if meta.flags != 0 =>
            java.lang.Long.valueOf(meta.flags).toString.getBytes
         case _ => ZERO
      }

      val flagsSize = flags.length
      val buf = buffer(VALUE_SIZE + key.length + data.length + flagsSize
      + dataSize.length + 6 + extraSpace)
      buf.writeBytes(VALUE)
      buf.writeBytes(key)
      buf.writeByte(SP)
      buf.writeBytes(flags)
      buf.writeByte(SP)
      buf.writeBytes(dataSize)
      buf
   }

   private def writeGetHeaderData(data: Array[Byte], buf: ByteBuf): ByteBuf = {
      buf.writeBytes(CRLFBytes)
      buf.writeBytes(data)
      buf.writeBytes(CRLFBytes)
      buf
   }

   private def writeGetHeaderEnd(buf: ByteBuf): ByteBuf = {
      buf.writeBytes(END)
      buf
   }

   private def buildSingleGetWithVersionResponse(k: String, entry: CacheEntry[String, Array[Byte]]): ByteBuf = {
      val v = entry.getValue
      // TODO: Would be nice for EntryVersion to allow retrieving the version itself...
      val version = entry.getMetadata.version().asInstanceOf[NumericVersion].getVersion.toString.getBytes
      val buf = buildGetHeaderBegin(k, entry, version.length + 1 + END_SIZE)
      buf.writeByte(SP) // 1
      buf.writeBytes(version) // version.length
      writeGetHeaderData(v, buf)
      writeGetHeaderEnd(buf)
   }

   private def friendlyMaxIntCheck(number: String, message: String): Int = {
      try {
         Integer.parseInt(number)
      } catch {
         case n: NumberFormatException => numericLimitCheck(number, Int.MaxValue, message, n)
      }
   }

   private def numericLimitCheck(number: String, maxValue: Long, message: String, n: NumberFormatException): Int = {
      if (java.lang.Long.parseLong(number) > maxValue)
         throw new NumberFormatException(message + " sent (" + number
         + ") exceeds the limit (" + maxValue + ")")
      else throw n
   }

   private def numericLimitCheck(number: String, maxValue: Long, message: String): Long = {
      val numeric = java.lang.Long.parseLong(number)
      if (java.lang.Long.parseLong(number) > maxValue)
         throw new NumberFormatException(message + " sent (" + number
         + ") exceeds the limit (" + maxValue + ")")
      numeric
   }
}

class MemcachedParameters(val valueLength: Int, val lifespan: Int,
                          val maxIdle: Int, val streamVersion: Long,
                          val noReply: Boolean, val flags: Long, val delta: String,
                          val flushDelay: Int) {
   override def toString = {
      new StringBuilder().append("MemcachedParameters").append("{")
      .append("valueLength=").append(valueLength)
      .append(", lifespan=").append(lifespan)
      .append(", maxIdle=").append(maxIdle)
      .append(", streamVersion=").append(streamVersion)
      .append(", noReply=").append(noReply)
      .append(", flags=").append(flags)
      .append(", delta=").append(delta)
      .append(", flushDelay=").append(flushDelay)
      .append("}").toString
   }
}

private class DelayedFlushAll(cache: AdvancedCache[String, Array[Byte]],
                              flushFunction: AdvancedCache[String, Array[Byte]] => Unit) extends Runnable {
   override def run() {
      flushFunction(cache.getAdvancedCache)
   }
}

private object RequestResolver extends Log {
   private val isTrace = isTraceEnabled

   def toRequest(commandName: String, endOfOp: Boolean, buffer: ByteBuf): Enumeration#Value = {
      if (isTrace) trace("Operation: '%s'", commandName)
      val op = commandName match {
         case "get" => GetRequest
         case "set" => PutRequest
         case "add" => PutIfAbsentRequest
         case "delete" => RemoveRequest
         case "replace" => ReplaceRequest
         case "cas" => ReplaceIfUnmodifiedRequest
         case "append" => AppendRequest
         case "prepend" => PrependRequest
         case "gets" => GetWithVersionRequest
         case "incr" => IncrementRequest
         case "decr" => DecrementRequest
         case "flush_all" => FlushAllRequest
         case "version" => VersionRequest
         case "stats" => StatsRequest
         case "verbosity" => VerbosityRequest
         case "quit" => QuitRequest
         case _ =>
            if (!endOfOp) {
               val line = readDiscardedLine(buffer) // Read rest of line to clear the operation
               debug("Unexpected operation '%s', rest of line contains: %s",
                  commandName, line)
            }

            throw new UnknownOperationException("Unknown operation: " + commandName)
      }
      op
   }
}

class MemcachedException(message: String, cause: Throwable) extends Exception(message, cause)

class RequestHeader {
   var op: Enumeration#Value = _

   override def toString = {
      new StringBuilder().append("RequestHeader").append("{")
      .append("op=").append(op)
      .append("}").toString
   }
}


class UnknownOperationException(reason: String) extends StreamCorruptedException(reason)

class PartialResponse(val buffer: Option[ByteBuf])
