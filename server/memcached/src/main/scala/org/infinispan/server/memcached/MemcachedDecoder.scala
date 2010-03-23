package org.infinispan.server.memcached

import org.infinispan.manager.{CacheManager}
import org.infinispan.server.core.Operation._
import org.infinispan.server.memcached.MemcachedOperation._
import org.infinispan.context.Flag
import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService}
import org.infinispan.{Version, CacheException, Cache}
import java.io.{IOException, EOFException, StreamCorruptedException}
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicLong
import org.infinispan.stats.Stats
import org.infinispan.server.core.transport.{Channel, ChannelBuffers, ChannelHandlerContext, ChannelBuffer}
import org.infinispan.server.core._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class MemcachedDecoder(cacheManager: CacheManager) extends AbstractProtocolDecoder[String, MemcachedValue] with TextProtocolUtil {
   import MemcachedDecoder._

   type SuitableParameters = MemcachedParameters

   private var scheduler: ScheduledExecutorService = _
   private var cache: Cache[String, MemcachedValue] = _
   private lazy val isStatsEnabled = cache.getConfiguration.isExposeJmxStatistics
   private final val incrMisses = new AtomicLong(0)
   private final val incrHits = new AtomicLong(0)
   private final val decrMisses = new AtomicLong(0)
   private final val decrHits = new AtomicLong(0)
   private final val replaceIfUnmodifiedMisses = new AtomicLong(0)
   private final val replaceIfUnmodifiedHits = new AtomicLong(0)
   private final val replaceIfUnmodifiedBadval = new AtomicLong(0)

   override def readHeader(buffer: ChannelBuffer): RequestHeader = {
      val streamOp = readElement(buffer)
      val op = OperationResolver.resolve(streamOp)
      if (op == None) {
         val line = readLine(buffer) // Read rest of line to clear the operation
         throw new UnknownOperationException("Unknown operation: " + streamOp);
      }
      new RequestHeader(op.get)
   }

   override def readKey(buffer: ChannelBuffer): String = {
      readElement(buffer)
   }

   override def readKeys(buffer: ChannelBuffer): Array[String] = {
      val line = readLine(buffer)
      line.trim.split(" +")
   }

   override def readParameters(op: Enumeration#Value, buffer: ChannelBuffer): MemcachedParameters = {
      val line = readLine(buffer)
      if (!line.isEmpty) {
         trace("Operation parameters: {0}", line)
         val args = line.trim.split(" +")
         var index = 0
         op match {
            case DeleteRequest => {
               val delayedDeleteTime = parseDelayedDeleteTime(index, args)
               val noReply = if (delayedDeleteTime == -1) parseNoReply(index, args) else false
               new MemcachedParameters(null, -1, -1, -1, noReply, 0, "", 0)
            }
            case IncrementRequest | DecrementRequest => {
               val delta = args(index)
               index += 1
               new MemcachedParameters(null, -1, -1, -1, parseNoReply(index, args), 0, delta, 0) 
            }
            case FlushAllRequest => {
               val flushDelay = args(index).toInt
               index += 1
               new MemcachedParameters(null, -1, -1, -1, parseNoReply(index, args), 0, "", flushDelay)
            }
            case _ => {
               val flags = getFlags(args(index))
               index += 1
               val lifespan = {
                  val streamLifespan = getLifespan(args(index))
                  if (streamLifespan <= 0) -1 else streamLifespan
               }
               index += 1
               val length = getLength(args(index))
               val streamVersion = op match {
                  case ReplaceIfUnmodifiedRequest => {
                     index += 1
                     getVersion(args(index))
                  }
                  case _ => -1
               }
               index += 1
               val noReply = parseNoReply(index, args)
               val data = new Array[Byte](length)
               buffer.readBytes(data, 0, data.length)
               readLine(buffer) // read the rest of line to clear CRLF after value Byte[]
               new MemcachedParameters(data, lifespan, -1, streamVersion, noReply, flags, "", 0)
            }
         }
      } else {
         // For example when delete <key> is sent without any further parameters
         new MemcachedParameters(null, -1, -1, -1, false, 0, "", 0)
      }
   }

   override def createValue(params: MemcachedParameters, nextVersion: Long): MemcachedValue = {
      new MemcachedValue(params.data, nextVersion, params.flags)
   }

   private def getFlags(flags: String): Int = {
      if (flags == null) throw new EOFException("No flags passed")
      flags.toInt
   }

   private def getLifespan(lifespan: String): Int = {
      if (lifespan == null) throw new EOFException("No expiry passed")
      lifespan.toInt
   }

   private def getLength(length: String): Int = {
      if (length == null) throw new EOFException("No bytes passed")
      length.toInt
   }

   private def getVersion(version: String): Long = {
      if (version == null) throw new EOFException("No cas passed")
      version.toLong
   }

   private def parseNoReply(expectedIndex: Int, args: Array[String]): Boolean = {
      if (args.length > expectedIndex) {
         if ("noreply" == args(expectedIndex))
            true
         else
            throw new StreamCorruptedException("Unable to parse noreply optional argument")
      }
      else false      
   }

   private def parseDelayedDeleteTime(expectedIndex: Int, args: Array[String]): Int = {
      if (args.length > expectedIndex) {
         try {
            args(expectedIndex).toInt
         }
         catch {
            case e: NumberFormatException => return -1 // Either unformatted number, or noreply found
         }
      }
      else 0
   }

   override def getCache(header: RequestHeader): Cache[String, MemcachedValue] = cache

   protected def createCache: Cache[String, MemcachedValue] = cacheManager.getCache[String, MemcachedValue]

   override def handleCustomRequest(header: RequestHeader, ctx: ChannelHandlerContext,
                                    buffer: ChannelBuffer, cache: Cache[String, MemcachedValue]) {
      header.op match {
         case AppendRequest | PrependRequest => {
            val k = readKey(buffer)
            val params = readParameters(header.op, buffer)
            val prev = cache.get(k)
            if (prev != null) {
               val concatenated = header.op match {
                  case AppendRequest => concat(prev.v, params.data);
                  case PrependRequest => concat(params.data, prev.v);
               }
               val next = createValue(concatenated, generateVersion(cache), params.flags)
               val replaced = cache.replace(k, prev, next);
               if (replaced)
                  sendResponse(header, ctx, None, None, Some(params), Some(prev))
               else // If there's a concurrent modification on this key, treat it as we couldn't replace it
                  sendResponse(header, ctx, None, None, Some(params), Some(null)) //
            } else {
               sendResponse(header, ctx, None, None, Some(params), Some(null))
            }
         }
         case IncrementRequest | DecrementRequest => {
            val k = readKey(buffer)
            val params = readParameters(header.op, buffer)
            val prev = cache.get(k)
            if (prev != null) {
               val prevCounter = new String(prev.v)
               val newCounter =
                  header.op match {
                     case IncrementRequest => prevCounter.toLong + params.delta.toLong
                     case DecrementRequest => {
                        val candidateCounter = prevCounter.toLong - params.delta.toLong
                        if (candidateCounter < 0) 0 else candidateCounter
                     }
                  }
               val next = createValue(newCounter.toString.getBytes, generateVersion(cache), params.flags)
               var replaced = cache.replace(k, prev, next)
               if (replaced)
                  sendResponse(header, ctx, None, Some(next), Some(params), Some(prev))
               else // If there's a concurrent modification on this key, the spec does not say what to do, so treat it as exceptional
                  throw new CacheException("Value modified since we retrieved from the cache, old value was " + prevCounter)
            } else {
               sendResponse(header, ctx, None, None, Some(params), Some(null))
            }
         }
         case FlushAllRequest => {
            val params = readParameters(header.op, buffer)
            val flushFunction = (cache: Cache[String, MemcachedValue]) => cache.getAdvancedCache.withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STORE).clear
            if (params.flushDelay == 0)
               // cache.getAdvancedCache.withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STORE).clear
               flushFunction(cache)
            else
               scheduler.schedule(new DelayedFlushAll(cache, flushFunction), params.flushDelay, TimeUnit.SECONDS)
            sendResponse(header, ctx, None, None, Some(params), None)
         }
         case VersionRequest => sendResponse(header, ctx, None, None, None, None)
//         case StatsRequest => 
      }
   }

   // todo: potentially move this up when implementing hotrod since only what's written might change, the rest of logic is likely to be the same
   override def sendResponse(header: RequestHeader, ctx: ChannelHandlerContext,
                             k: Option[String], v: Option[MemcachedValue],
                             params: Option[MemcachedParameters], prev: Option[MemcachedValue]) {
      val buffers = ctx.getChannelBuffers
      val ch = ctx.getChannel
      if (params == None || !params.get.noReply) {
         header.op match {
            case PutRequest => ch.write(buffers.wrappedBuffer("STORED\r\n".getBytes))
            case GetRequest | GetWithVersionRequest => {
               if (v.get != null)
                  ch.write(buildGetResponse(header.op, ctx, k.get, v.get))
               ch.write(buffers.wrappedBuffer("END\r\n".getBytes))
            }
            case PutIfAbsentRequest => {
               if (prev.get == null)
                  ch.write(buffers.wrappedBuffer("STORED\r\n".getBytes))
               else
                  ch.write(buffers.wrappedBuffer("NOT_STORED\r\n".getBytes))
            }
            case ReplaceRequest | AppendRequest | PrependRequest => {
               if (prev.get == null)
                  ch.write(buffers.wrappedBuffer("NOT_STORED\r\n".getBytes))
               else
                  ch.write(buffers.wrappedBuffer("STORED\r\n".getBytes))
            }
            case ReplaceIfUnmodifiedRequest => {
               if (v != None && prev != None) {
                  if (isStatsEnabled) replaceIfUnmodifiedHits.incrementAndGet
                  ch.write(buffers.wrappedBuffer("STORED\r\n".getBytes))
               } else if (v == None && prev != None) {
                  if (isStatsEnabled) replaceIfUnmodifiedBadval.incrementAndGet
                  ch.write(buffers.wrappedBuffer("EXISTS\r\n".getBytes))
               } else {
                  if (isStatsEnabled) replaceIfUnmodifiedMisses.incrementAndGet
                  ch.write(buffers.wrappedBuffer("NOT_FOUND\r\n".getBytes))
               }                  
            }
            case DeleteRequest => {
               if (prev.get == null)
                  ch.write(buffers.wrappedBuffer("NOT_FOUND\r\n".getBytes))
               else
                  ch.write(buffers.wrappedBuffer("DELETED\r\n".getBytes))
            }
            case IncrementRequest | DecrementRequest => {
               if (prev.get == null) {
                  if (isStatsEnabled) if (header.op == IncrementRequest) incrMisses.incrementAndGet() else decrMisses.incrementAndGet
                  ch.write(buffers.wrappedBuffer("NOT_FOUND\r\n".getBytes))
               } else {
                  if (isStatsEnabled) if (header.op == IncrementRequest) incrHits.incrementAndGet() else decrHits.incrementAndGet
                  ch.write(buffers.wrappedBuffer((new String(v.get.v) + CRLF).getBytes))
               }
            }
            case FlushAllRequest => {
               ch.write(buffers.wrappedBuffer("OK\r\n".getBytes))
            }
            case VersionRequest => {
               val sb = new StringBuilder
               sb.append("VERSION ").append(Version.version).append(CRLF)
               ch.write(buffers.wrappedBuffer(sb.toString.getBytes))
            }
         }
      }
   }

   override def sendResponse(header: RequestHeader, ctx: ChannelHandlerContext, pairs: Map[String, MemcachedValue]) {
      val buffers = ctx.getChannelBuffers
      val ch = ctx.getChannel
         header.op match {
            case GetRequest | GetWithVersionRequest => {
               for ((k, v) <- pairs)
                  ch.write(buildGetResponse(header.op, ctx, k, v))
               ch.write(buffers.wrappedBuffer("END\r\n".getBytes))
            }
      }
   }

   override def sendResponse(ctx: ChannelHandlerContext, t: Throwable) {
      val ch = ctx.getChannel
      val buffers = ctx.getChannelBuffers
      t match {
         case uoe: UnknownOperationException => ch.write(buffers.wrappedBuffer("ERROR\r\n".getBytes))
         case cce: ClosedChannelException => // no-op, only log
         case _ => {
            val sb = new StringBuilder
            t match {
               case ioe: IOException => sb.append("CLIENT_ERROR ")
               case _ => sb.append("SERVER_ERROR ")
            }
            sb.append(t).append(CRLF)
            ch.write(buffers.wrappedBuffer(sb.toString.getBytes))
         }
      }
   }

   def sendResponse(ctx: ChannelHandlerContext, stats: Stats) {
      var buffers = ctx.getChannelBuffers
      var ch = ctx.getChannel
      var sb = new StringBuilder

      writeStat("pid", 0, sb, buffers, ch) // Unsupported
      writeStat("uptime", stats.getTimeSinceStart, sb, buffers, ch)
      writeStat("time", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis), sb, buffers, ch)
      writeStat("version", cache.getVersion, sb, buffers, ch)
      writeStat("pointer_size", 0, sb, buffers, ch) // Unsupported
      writeStat("rusage_user", 0, sb, buffers, ch) // Unsupported
      writeStat("rusage_system", 0, sb, buffers, ch) // Unsupported
      writeStat("curr_items", stats.getCurrentNumberOfEntries, sb, buffers, ch)
      writeStat("total_items", stats.getTotalNumberOfEntries, sb, buffers, ch)
      writeStat("bytes", 0, sb, buffers, ch) // Unsupported
      writeStat("curr_connections", 0, sb, buffers, ch) // TODO: Through netty?
      writeStat("total_connections", 0, sb, buffers, ch) // TODO: Through netty?
      writeStat("connection_structures", 0, sb, buffers, ch) // Unsupported
      writeStat("cmd_get", stats.getRetrievals, sb, buffers, ch)
      writeStat("cmd_set", stats.getStores, sb, buffers, ch)
      writeStat("get_hits", stats.getHits, sb, buffers, ch)
      writeStat("get_misses", stats.getMisses, sb, buffers, ch)
      writeStat("delete_misses", stats.getRemoveMisses, sb, buffers, ch)
      writeStat("delete_hits", stats.getRemoveHits, sb, buffers, ch)
      writeStat("incr_misses", incrMisses, sb, buffers, ch)
      writeStat("incr_hits", incrHits, sb, buffers, ch)
      writeStat("decr_misses", decrMisses, sb, buffers, ch)
      writeStat("decr_hits", decrHits, sb, buffers, ch)
      writeStat("cas_misses", replaceIfUnmodifiedMisses, sb, buffers, ch)
      writeStat("cas_hits", replaceIfUnmodifiedHits, sb, buffers, ch)
      writeStat("cas_badval", replaceIfUnmodifiedBadval, sb, buffers, ch)
      writeStat("auth_cmds", 0, sb, buffers, ch) // Unsupported
      writeStat("auth_errors", 0, sb, buffers, ch) // Unsupported
      //TODO: Evictions are measure by evict calls, but not by nodes are that are expired after the entry's lifespan has expired.
      writeStat("evictions", stats.getEvictions, sb, buffers, ch)
      writeStat("bytes_read", 0, sb, buffers, ch) // TODO: Through netty?
      writeStat("bytes_written", 0, sb, buffers, ch) // TODO: Through netty?
      writeStat("limit_maxbytes", 0, sb, buffers, ch) // Unsupported
      writeStat("threads", 0, sb, buffers, ch) // TODO: Through netty?
      writeStat("conn_yields", 0, sb, buffers, ch) // Unsupported

      ch.write(buffers.wrappedBuffer("END\r\n".getBytes))
   }

   private def writeStat(stat: String, value: Any, sb: StringBuilder, buffers: ChannelBuffers, ch: Channel) {
      sb.append("STAT").append(' ').append(stat).append(' ').append(value).append(CRLF)
      ch.write(buffers.wrappedBuffer(sb.toString.getBytes))
      sb.setLength(0)
   }

   override def start {
      scheduler = Executors.newScheduledThreadPool(1)
      cache = createCache
   }

   override def stop {
      scheduler.shutdown
   }

   private def createValue(data: Array[Byte], nextVersion: Long, flags: Int): MemcachedValue = {
      new MemcachedValue(data, nextVersion, flags)
   }   

   private def buildGetResponse(op: Enumeration#Value, ctx: ChannelHandlerContext,
                                k: String, v: MemcachedValue): ChannelBuffer = {
      val header = buildGetResponseHeader(k, v, op)
      ctx.getChannelBuffers.wrappedBuffer(header.getBytes, v.v, CRLF.getBytes)
   }

   private def buildGetResponseHeader(k: String, v: MemcachedValue, op: Enumeration#Value): String = {
      val sb = new StringBuilder
      sb.append("VALUE ").append(k).append(" ").append(v.flags).append(" ").append(v.v.length).append(" ")
      if (op == GetWithVersionRequest)
         sb.append(v.version).append(" ")   
      sb.append(CRLF)
      sb.toString
   }

}

object MemcachedDecoder extends Logging

class MemcachedParameters(override val data: Array[Byte], override val lifespan: Int,
                          override val maxIdle: Int, override val version: Long,
                          val noReply: Boolean, val flags: Int, val delta: String,
                          val flushDelay: Int) extends RequestParameters(data, lifespan, maxIdle, version)

private class DelayedFlushAll(cache: Cache[String, MemcachedValue],
                              flushFunction: Cache[String, MemcachedValue] => Unit) extends Runnable {
   override def run() = flushFunction(cache)
}


