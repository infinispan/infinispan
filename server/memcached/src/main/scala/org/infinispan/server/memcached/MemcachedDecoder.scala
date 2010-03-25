package org.infinispan.server.memcached

import org.infinispan.manager.{CacheManager}
import org.infinispan.server.core.Operation._
import org.infinispan.server.memcached.MemcachedOperation._
import org.infinispan.context.Flag
import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService}
import java.io.{IOException, EOFException, StreamCorruptedException}
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicLong
import org.infinispan.stats.Stats
import org.infinispan.server.core.transport.{Channel, ChannelBuffers, ChannelHandlerContext, ChannelBuffer}
import org.infinispan.server.core._
import org.infinispan.{AdvancedCache, Version, CacheException, Cache}
import collection.mutable.ListBuffer

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class MemcachedDecoder(cacheManager: CacheManager) extends AbstractProtocolDecoder[String, MemcachedValue] with TextProtocolUtil {
   import RequestResolver._

   type SuitableParameters = MemcachedParameters
   type SuitableHeader = RequestHeader

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
      val op = toRequest(streamOp)
      if (op == None) {
         val line = readLine(buffer) // Read rest of line to clear the operation
         throw new UnknownOperationException("Unknown operation: " + streamOp);
      }
      new RequestHeader(op.get)
   }

   override def readKey(header: RequestHeader, buffer: ChannelBuffer): String = {
      readElement(buffer)
   }

   override def readKeys(header: RequestHeader, buffer: ChannelBuffer): Array[String] = {
      val line = readLine(buffer)
      line.trim.split(" +")
   }

   override def readParameters(header: RequestHeader, buffer: ChannelBuffer): Option[MemcachedParameters] = {
      val line = readLine(buffer)
      if (!line.isEmpty) {
         trace("Operation parameters: {0}", line)
         val args = line.trim.split(" +")
         var index = 0
         header.op match {
            case RemoveRequest => {
               val delayedDeleteTime = parseDelayedDeleteTime(index, args)
               val noReply = if (delayedDeleteTime == -1) parseNoReply(index, args) else false
               Some(new MemcachedParameters(null, -1, -1, -1, noReply, 0, "", 0))
            }
            case IncrementRequest | DecrementRequest => {
               val delta = args(index)
               index += 1
               Some(new MemcachedParameters(null, -1, -1, -1, parseNoReply(index, args), 0, delta, 0)) 
            }
            case FlushAllRequest => {
               val flushDelay = args(index).toInt
               index += 1
               Some(new MemcachedParameters(null, -1, -1, -1, parseNoReply(index, args), 0, "", flushDelay))
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
               val streamVersion = header.op match {
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
               Some(new MemcachedParameters(data, lifespan, -1, streamVersion, noReply, flags, "", 0))
            }
         }
      } else {
         None // For example when delete <key> is sent without any further parameters, or flush_all without delay
      }
   }

   override def createValue(header: SuitableHeader, params: MemcachedParameters, nextVersion: Long): MemcachedValue = {
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
                                    buffer: ChannelBuffer, cache: Cache[String, MemcachedValue]): AnyRef = {
      val ch = ctx.getChannel
      val buffers = ctx.getChannelBuffers
      header.op match {
         case AppendRequest | PrependRequest => {
            val k = readKey(header, buffer)
            val params = readParameters(header, buffer)
            val prev = cache.get(k)
            if (prev != null) {
               val concatenated = header.op match {
                  case AppendRequest => concat(prev.data, params.get.data);
                  case PrependRequest => concat(params.get.data, prev.data);
               }
               val next = createValue(concatenated, generateVersion(cache), params.get.flags)
               val replaced = cache.replace(k, prev, next);
               if (replaced)
                  if (!params.get.noReply) STORED else null
               else // If there's a concurrent modification on this key, treat it as we couldn't replace it
                  if (!params.get.noReply) NOT_STORED else null
            } else {
               if (!params.get.noReply) NOT_STORED else null
            }
         }
         case IncrementRequest | DecrementRequest => {
            val k = readKey(header, buffer)
            val params = readParameters(header, buffer)
            val prev = cache.get(k)
            if (prev != null) {
               val prevCounter = new String(prev.data)
               val newCounter =
                  header.op match {
                     case IncrementRequest => prevCounter.toLong + params.get.delta.toLong
                     case DecrementRequest => {
                        val candidateCounter = prevCounter.toLong - params.get.delta.toLong
                        if (candidateCounter < 0) 0 else candidateCounter
                     }
                  }
               val next = createValue(newCounter.toString.getBytes, generateVersion(cache), params.get.flags)
               var replaced = cache.replace(k, prev, next)
               if (replaced) {
                  if (isStatsEnabled) if (header.op == IncrementRequest) incrHits.incrementAndGet() else decrHits.incrementAndGet
                  if (!params.get.noReply) new String(next.data) + CRLF else null
               } else {
                  // If there's a concurrent modification on this key, the spec does not say what to do, so treat it as exceptional
                  throw new CacheException("Value modified since we retrieved from the cache, old value was " + prevCounter)
               }
            } else {
               if (isStatsEnabled) if (header.op == IncrementRequest) incrMisses.incrementAndGet() else decrMisses.incrementAndGet
               if (!params.get.noReply) NOT_FOUND else null
            }
         }
         case FlushAllRequest => {
            val params = readParameters(header, buffer)
            val flushFunction = (cache: AdvancedCache[String, MemcachedValue]) => cache.withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STORE).clear
            val flushDelay = if (params == None) 0 else params.get.flushDelay
            if (flushDelay == 0)
               flushFunction(cache.getAdvancedCache)
            else
               scheduler.schedule(new DelayedFlushAll(cache, flushFunction), flushDelay, TimeUnit.SECONDS)
            if (params == None || !params.get.noReply) OK else null
         }
         case VersionRequest => new StringBuilder().append("VERSION ").append(Version.version).append(CRLF)
      }
   }

//   override def createPutResponse(header: RequestHeader): AnyRef = STORED

   override def createSuccessResponse(header: RequestHeader): AnyRef = {
      if (isStatsEnabled) {
         header.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedHits.incrementAndGet
            case _ => // No-op
         }
      }
      header.op match {
         case RemoveRequest => DELETED
         case _ => STORED
      }
   }

   override def createNotExecutedResponse(header: RequestHeader): AnyRef = {
      if (isStatsEnabled) {
         header.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedBadval.incrementAndGet
            case _ => // No-op
         }
      }
      header.op match {
         case ReplaceIfUnmodifiedRequest => EXISTS
         case _ => NOT_STORED
      }
   }

   override def createNotExistResponse(header: SuitableHeader): AnyRef = {
      if (isStatsEnabled) {
         header.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedMisses.incrementAndGet
            case _ => // No-op
         }
      }      
      NOT_FOUND
   }

   override def createGetResponse(header: RequestHeader, buffers: ChannelBuffers,
                                  k: String, v: MemcachedValue): AnyRef = {
      if (v != null)
         List(buildGetResponse(header.op, buffers, k, v), buffers.wrappedBuffer(END))
      else
         END
   }

   override def createMultiGetResponse(header: RequestHeader, buffers: ChannelBuffers,
                                       pairs: Map[String, MemcachedValue]): AnyRef = {
      val elements = new ListBuffer[ChannelBuffer]
      header.op match {
         case GetRequest | GetWithVersionRequest => {
            for ((k, v) <- pairs)
               elements += buildGetResponse(header.op, buffers, k, v)
            elements += buffers.wrappedBuffer("END\r\n".getBytes)
         }
      }
      elements.toList
   }

   override def createErrorResponse(t: Throwable): AnyRef = {
      val sb = new StringBuilder
      t match {
         case se: ServerException => {
            se.getCause match {
               case uoe: UnknownOperationException => ERROR
               case cce: ClosedChannelException => null// no-op, only log
               case _ => {
                  t match {
                     case ioe: IOException => sb.append("CLIENT_ERROR ")
                     case _ => sb.append("SERVER_ERROR ")
                  }
                  sb.append(t).append(CRLF)
               }
            }
         }
         case _ => sb.append("SERVER_ERROR ").append(t).append(CRLF)
      }
   }

   def createStatsResponse(header: RequestHeader, buffers: ChannelBuffers, stats: Stats): AnyRef = {
      var sb = new StringBuilder
      List[ChannelBuffer] (
         buildStat("pid", 0, sb, buffers),
         buildStat("uptime", stats.getTimeSinceStart, sb, buffers),
         buildStat("uptime", stats.getTimeSinceStart, sb, buffers),
         buildStat("time", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis), sb, buffers),
         buildStat("version", cache.getVersion, sb, buffers),
         buildStat("pointer_size", 0, sb, buffers), // Unsupported
         buildStat("rusage_user", 0, sb, buffers), // Unsupported
         buildStat("rusage_system", 0, sb, buffers), // Unsupported
         buildStat("curr_items", stats.getCurrentNumberOfEntries, sb, buffers),
         buildStat("total_items", stats.getTotalNumberOfEntries, sb, buffers),
         buildStat("bytes", 0, sb, buffers), // Unsupported
         buildStat("curr_connections", 0, sb, buffers), // TODO: Through netty?
         buildStat("total_connections", 0, sb, buffers), // TODO: Through netty?
         buildStat("connection_structures", 0, sb, buffers), // Unsupported
         buildStat("cmd_get", stats.getRetrievals, sb, buffers),
         buildStat("cmd_set", stats.getStores, sb, buffers),
         buildStat("get_hits", stats.getHits, sb, buffers),
         buildStat("get_misses", stats.getMisses, sb, buffers),
         buildStat("delete_misses", stats.getRemoveMisses, sb, buffers),
         buildStat("delete_hits", stats.getRemoveHits, sb, buffers),
         buildStat("incr_misses", incrMisses, sb, buffers),
         buildStat("incr_hits", incrHits, sb, buffers),
         buildStat("decr_misses", decrMisses, sb, buffers),
         buildStat("decr_hits", decrHits, sb, buffers),
         buildStat("cas_misses", replaceIfUnmodifiedMisses, sb, buffers),
         buildStat("cas_hits", replaceIfUnmodifiedHits, sb, buffers),
         buildStat("cas_badval", replaceIfUnmodifiedBadval, sb, buffers),
         buildStat("auth_cmds", 0, sb, buffers), // Unsupported
         buildStat("auth_errors", 0, sb, buffers), // Unsupported
         //TODO: Evictions are measure by evict calls, but not by nodes are that are expired after the entry's lifespan has expired.
         buildStat("evictions", stats.getEvictions, sb, buffers),
         buildStat("bytes_read", 0, sb, buffers), // TODO: Through netty?
         buildStat("bytes_written", 0, sb, buffers), // TODO: Through netty?
         buildStat("limit_maxbytes", 0, sb, buffers), // Unsupported
         buildStat("threads", 0, sb, buffers), // TODO: Through netty?
         buildStat("conn_yields", 0, sb, buffers), // Unsupported
         buffers.wrappedBuffer(END)
      )
   }

   private def buildStat(stat: String, value: Any, sb: StringBuilder, buffers: ChannelBuffers): ChannelBuffer = {
      sb.append("STAT").append(' ').append(stat).append(' ').append(value).append(CRLF)
      val buffer = buffers.wrappedBuffer(sb.toString.getBytes)
      sb.setLength(0)
      buffer
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

   private def buildGetResponse(op: Enumeration#Value, buffers: ChannelBuffers,
                                k: String, v: MemcachedValue): ChannelBuffer = {
      val header = buildGetResponseHeader(k, v, op)
      buffers.wrappedBuffer(header.getBytes, v.data, CRLFBytes)
   }

   private def buildGetResponseHeader(k: String, v: MemcachedValue, op: Enumeration#Value): String = {
      val sb = new StringBuilder
      sb.append("VALUE ").append(k).append(" ").append(v.flags).append(" ").append(v.data.length).append(" ")
      if (op == GetWithVersionRequest)
         sb.append(v.version).append(" ")   
      sb.append(CRLF)
      sb.toString
   }

}

class MemcachedParameters(override val data: Array[Byte], override val lifespan: Int,
                          override val maxIdle: Int, override val streamVersion: Long,
                          override val noReply: Boolean, val flags: Int, val delta: String,
                          val flushDelay: Int) extends RequestParameters(data, lifespan, maxIdle, streamVersion, noReply)

private class DelayedFlushAll(cache: Cache[String, MemcachedValue],
                              flushFunction: AdvancedCache[String, MemcachedValue] => Unit) extends Runnable {
   override def run() = flushFunction(cache.getAdvancedCache)
}

private object RequestResolver extends Logging {
   private val operations = Map[String, Enumeration#Value](
      "set" -> PutRequest,
      "add" -> PutIfAbsentRequest,
      "replace" -> ReplaceRequest,
      "cas" -> ReplaceIfUnmodifiedRequest,
      "append" -> AppendRequest,
      "prepend" -> PrependRequest,
      "get" -> GetRequest,
      "gets" -> GetWithVersionRequest,
      "delete" -> RemoveRequest,
      "incr" -> IncrementRequest,
      "decr" -> DecrementRequest,
      "flush_all" -> FlushAllRequest,
      "version" -> VersionRequest,
      "stats" -> StatsRequest
   )

   def toRequest(commandName: String): Option[Enumeration#Value] = {
      trace("Operation: {0}", commandName)
      val op = operations.get(commandName)
      op
   }
}

