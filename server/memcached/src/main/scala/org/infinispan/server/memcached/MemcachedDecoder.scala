package org.infinispan.server.memcached

import org.infinispan.manager.{CacheContainer}
import org.infinispan.server.core.Operation._
import org.infinispan.server.memcached.MemcachedOperation._
import org.infinispan.context.Flag
import java.util.concurrent.{TimeUnit, ScheduledExecutorService}
import java.io.{IOException, EOFException, StreamCorruptedException}
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicLong
import org.infinispan.stats.Stats
import org.infinispan.server.core.transport.ChannelBuffer
import org.infinispan.server.core._
import org.infinispan.{AdvancedCache, Version, CacheException, Cache}
import org.infinispan.server.core.transport.ChannelBuffers._
import org.infinispan.util.Util
import collection.mutable.{HashMap, ListBuffer}
import scala.collection.immutable

/**
 * A Memcached protocol specific decoder
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class MemcachedDecoder(cache: Cache[String, MemcachedValue], scheduler: ScheduledExecutorService)
      extends AbstractProtocolDecoder[String, MemcachedValue] with TextProtocolUtil {
   import RequestResolver._

   type SuitableParameters = MemcachedParameters
   type SuitableHeader = RequestHeader

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

   override def readKey(h: RequestHeader, b: ChannelBuffer): String = {
      val k = readElement(b)
      if (k.length > 250) throw new ServerException(h, new IOException("Key length over the 250 character limit")) else k
   }

   private def readKeys(h: RequestHeader, b: ChannelBuffer): Array[String] = {
      val line = readLine(b)
      line.trim.split(" +")
   }

   override protected def get(h: RequestHeader, buffer: ChannelBuffer, cache: Cache[String, MemcachedValue]): AnyRef = {
      val keys = readKeys(h, buffer)
      if (keys.length > 1) {
         val map = new HashMap[String, MemcachedValue]()
         for (k <- keys) {
            val v = cache.get(k)
            if (v != null)
               map += (k -> v)
         }
         createMultiGetResponse(h, new immutable.HashMap ++ map)
      } else {
         createGetResponse(h, keys.head, cache.get(keys.head))
      }
   }

   override def readParameters(h: RequestHeader, b: ChannelBuffer): Option[MemcachedParameters] = {
      val line = readLine(b)
      if (!line.isEmpty) {
         if (isTraceEnabled) trace("Operation parameters: {0}", line)
         val args = line.trim.split(" +")
         try {
            var index = 0
            h.op match {
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
                  val streamVersion = h.op match {
                     case ReplaceIfUnmodifiedRequest => {
                        index += 1
                        getVersion(args(index))
                     }
                     case _ => -1
                  }
                  index += 1
                  val noReply = parseNoReply(index, args)
                  val data = new Array[Byte](length)
                  b.readBytes(data, 0, data.length)
                  readLine(b) // read the rest of line to clear CRLF after value Byte[]
                  Some(new MemcachedParameters(data, lifespan, -1, streamVersion, noReply, flags, "", 0))
               }
            }
         } catch {
            case _: ArrayIndexOutOfBoundsException => throw new IOException("Missing content in command line " + line)
         }
      } else {
         None // For example when delete <key> is sent without any further parameters, or flush_all without delay
      }
   }

   override def createValue(h: SuitableHeader, p: MemcachedParameters, nextVersion: Long): MemcachedValue = {
      new MemcachedValue(p.data, nextVersion, p.flags)
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

   override def getCache(h: RequestHeader): Cache[String, MemcachedValue] = cache

   override def handleCustomRequest(h: RequestHeader, b: ChannelBuffer, cache: Cache[String, MemcachedValue]): AnyRef = {
      h.op match {
         case AppendRequest | PrependRequest => {
            val k = readKey(h, b)
            val params = readParameters(h, b)
            val prev = cache.get(k)
            if (prev != null) {
               val concatenated = h.op match {
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
            val k = readKey(h, b)
            val params = readParameters(h, b)
            val prev = cache.get(k)
            if (prev != null) {
               val prevCounter = BigInt(new String(prev.data))
               val newCounter =
                  h.op match {
                     case IncrementRequest => {
                        val candidateCounter = prevCounter + BigInt(params.get.delta)
                        if (candidateCounter > BigInt("18446744073709551615")) 0 else candidateCounter
                     }
                     case DecrementRequest => {
                        val candidateCounter = prevCounter - BigInt(params.get.delta)
                        if (candidateCounter < 0) 0 else candidateCounter
                     }
                  }
               val next = createValue(newCounter.toString.getBytes, generateVersion(cache), params.get.flags)
               var replaced = cache.replace(k, prev, next)
               if (replaced) {
                  if (isStatsEnabled) if (h.op == IncrementRequest) incrHits.incrementAndGet() else decrHits.incrementAndGet
                  if (!params.get.noReply) new String(next.data) + CRLF else null
               } else {
                  // If there's a concurrent modification on this key, the spec does not say what to do, so treat it as exceptional
                  throw new CacheException("Value modified since we retrieved from the cache, old value was " + prevCounter)
               }
            } else {
               if (isStatsEnabled) if (h.op == IncrementRequest) incrMisses.incrementAndGet() else decrMisses.incrementAndGet
               if (!params.get.noReply) NOT_FOUND else null
            }
         }
         case FlushAllRequest => {
            val params = readParameters(h, b)
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

   override def createSuccessResponse(h: RequestHeader, params: Option[MemcachedParameters], prev: MemcachedValue): AnyRef = {
      if (isStatsEnabled) {
         h.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedHits.incrementAndGet
            case _ => // No-op
         }
      }
      if (params == None || !params.get.noReply) {
         h.op match {
            case RemoveRequest => DELETED
            case _ => STORED
         }
      } else null
   }

   override def createNotExecutedResponse(h: RequestHeader, params: Option[MemcachedParameters], prev: MemcachedValue): AnyRef = {
      if (isStatsEnabled) {
         h.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedBadval.incrementAndGet
            case _ => // No-op
         }
      }
      if (params == None || !params.get.noReply) {
         h.op match {
            case ReplaceIfUnmodifiedRequest => EXISTS
            case _ => NOT_STORED
         }
      } else null
   }

   override def createNotExistResponse(h: SuitableHeader, params: Option[MemcachedParameters]): AnyRef = {
      if (isStatsEnabled) {
         h.op match {
            case ReplaceIfUnmodifiedRequest => replaceIfUnmodifiedMisses.incrementAndGet
            case _ => // No-op
         }
      }
      if (params == None || !params.get.noReply)
         NOT_FOUND
      else
         null
   }

   override def createGetResponse(h: RequestHeader, k: String, v: MemcachedValue): AnyRef = {
      if (v != null)
         List(buildGetResponse(h.op, k, v), wrappedBuffer(END))
      else
         END
   }

   override def createMultiGetResponse(h: RequestHeader, pairs: Map[String, MemcachedValue]): AnyRef = {
      val elements = new ListBuffer[ChannelBuffer]
      h.op match {
         case GetRequest | GetWithVersionRequest => {
            for ((k, v) <- pairs)
               elements += buildGetResponse(h.op, k, v)
            elements += wrappedBuffer("END\r\n".getBytes)
         }
      }
      elements.toList
   }

   override def createErrorResponse(t: Throwable): AnyRef = {
      val sb = new StringBuilder
      t match {
         case se: ServerException => {
            val cause = se.getCause
            cause match {
               case u: UnknownOperationException => ERROR
               case c: ClosedChannelException => null // no-op, only log
               case _ => {
                  cause match {
                     case i: IOException => sb.append("CLIENT_ERROR bad command line format: ")
                     case n: NumberFormatException => sb.append("CLIENT_ERROR bad command line format: ")
                     case _ => sb.append("SERVER_ERROR ")
                  }
                  sb.append(t).append(CRLF)
               }
            }
         }
         case c: ClosedChannelException => null // no-op, only log
         case _ => sb.append("SERVER_ERROR ").append(t).append(CRLF)
      }
   }

   def createStatsResponse(header: RequestHeader, stats: Stats): AnyRef = {
      var sb = new StringBuilder
      List[ChannelBuffer] (
         buildStat("pid", 0, sb),
         buildStat("uptime", stats.getTimeSinceStart, sb),
         buildStat("uptime", stats.getTimeSinceStart, sb),
         buildStat("time", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis), sb),
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
         buildStat("bytes_read", 0, sb), // TODO: Through netty?
         buildStat("bytes_written", 0, sb), // TODO: Through netty?
         buildStat("limit_maxbytes", 0, sb), // Unsupported
         buildStat("threads", 0, sb), // TODO: Through netty?
         buildStat("conn_yields", 0, sb), // Unsupported
         wrappedBuffer(END)
      )
   }

   private def buildStat(stat: String, value: Any, sb: StringBuilder): ChannelBuffer = {
      sb.append("STAT").append(' ').append(stat).append(' ').append(value).append(CRLF)
      val buffer = wrappedBuffer(sb.toString.getBytes)
      sb.setLength(0)
      buffer
   }

   private def createValue(data: Array[Byte], nextVersion: Long, flags: Int): MemcachedValue = {
      new MemcachedValue(data, nextVersion, flags)
   }   

   private def buildGetResponse(op: Enumeration#Value, k: String, v: MemcachedValue): ChannelBuffer = {
      val header = buildGetResponseHeader(k, v, op)
      wrappedBuffer(header.getBytes, v.data, CRLFBytes)
   }

   private def buildGetResponseHeader(k: String, v: MemcachedValue, op: Enumeration#Value): String = {
      val sb = new StringBuilder
      sb.append("VALUE ").append(k).append(" ").append(v.flags).append(" ").append(v.data.length)
      if (op == GetWithVersionRequest)
         sb.append(" ").append(v.version)
      sb.append(CRLF)
      sb.toString
   }

}

class MemcachedParameters(override val data: Array[Byte], override val lifespan: Int,
                          override val maxIdle: Int, override val streamVersion: Long,
                          val noReply: Boolean, val flags: Int, val delta: String,
                          val flushDelay: Int) extends RequestParameters(data, lifespan, maxIdle, streamVersion) {
   override def toString = {
      new StringBuilder().append("MemcachedParameters").append("{")
         .append("data=").append(Util.printArray(data, true))
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
      if (isTraceEnabled) trace("Operation: {0}", commandName)
      val op = operations.get(commandName)
      op
   }
}

