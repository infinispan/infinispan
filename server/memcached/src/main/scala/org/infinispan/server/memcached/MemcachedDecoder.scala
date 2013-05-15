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
package org.infinispan.server.memcached

import logging.Log
import org.infinispan.server.core.Operation._
import org.infinispan.server.memcached.MemcachedOperation._
import org.infinispan.context.Flag
import java.util.concurrent.{TimeUnit, ScheduledExecutorService}
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicLong
import org.infinispan.server.core._
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import org.infinispan._
import collection.mutable.ListBuffer
import collection.{mutable, immutable}
import org.jboss.netty.buffer.ChannelBuffer
import transport.NettyTransport
import DecoderState._
import java.lang.StringBuilder
import java.io.{ByteArrayOutputStream, IOException, EOFException, StreamCorruptedException}
import org.jboss.netty.channel.Channel
import org.infinispan.server.memcached.TextProtocolUtil._
import scala.Predef._
import org.infinispan.container.entries.CacheEntry
import scala.Some
import org.infinispan.server.core.ServerEntryVersion

/**
 * A Memcached protocol specific decoder
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class MemcachedDecoder(memcachedCache: AdvancedCache[String, Array[Byte]], scheduler: ScheduledExecutorService, transport: NettyTransport)
      extends AbstractProtocolDecoder[String, Array[Byte]](transport) {

   cache = memcachedCache.getAdvancedCache.withFlags(Flag.OPERATION_MEMCACHED)

   import RequestResolver._

   type SuitableParameters = MemcachedParameters
   type SuitableHeader = RequestHeader

   private lazy val isStatsEnabled =
      cache.getCacheConfiguration.jmxStatistics().enabled()
   private final val incrMisses = new AtomicLong(0)
   private final val incrHits = new AtomicLong(0)
   private final val decrMisses = new AtomicLong(0)
   private final val decrHits = new AtomicLong(0)
   private final val replaceIfUnmodifiedMisses = new AtomicLong(0)
   private final val replaceIfUnmodifiedHits = new AtomicLong(0)
   private final val replaceIfUnmodifiedBadval = new AtomicLong(0)
   private val isTrace = isTraceEnabled
   private val byteBuffer = new ByteArrayOutputStream()

   override def createHeader: RequestHeader = new RequestHeader

   override def readHeader(buffer: ChannelBuffer, header: RequestHeader): Option[Boolean] = {
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

   override def readKey(b: ChannelBuffer): (String, Boolean) = {
      val endOfOp = readElement(b, byteBuffer)
      val k = extractString(byteBuffer)
      checkKeyLength(k, endOfOp, b)
      (k, endOfOp)
   }

   private def readKeys(b: ChannelBuffer): Seq[String] = readSplitLine(b)

   override protected def get(buffer: ChannelBuffer): AnyRef = {
      val keys = readKeys(buffer)
      if (keys.length > 1) {
         val map = new mutable.HashMap[String, CacheEntry]()
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

   private def checkKeyLength(k: String, endOfOp: Boolean, b: ChannelBuffer): String = {
      if (k.length > 250) {
         if (!endOfOp) skipLine(b) // Clear the rest of line
         throw new StreamCorruptedException("Key length over the 250 character limit")
      } else k
   }

   override def readParameters(ch: Channel, b: ChannelBuffer): Boolean = {
      val args = readSplitLine(b)
      var endOfOp = false
      params =
         if (!args.isEmpty) {
            if (isTrace) trace("Operation parameters: %s", args)
            try {
               header.op match {
                  case PutRequest => readStorageParameters(args, b)
                  case RemoveRequest => readRemoveParameters(args)
                  case IncrementRequest | DecrementRequest => {
                     endOfOp = true
                     readIncrDecrParameters(args)
                  }
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
            case n: NumberFormatException => {
               if (n.getMessage.contains("noreply")) {
                  noReplyFound = true
                  0
               } else throw n
            }
         }
      val noReply = if (!noReplyFound) parseNoReply(1, args) else true
      new MemcachedParameters(-1, -1, -1, -1, noReply, 0, "", flushDelay)
   }

   private def readStorageParameters(args: Seq[String], b: ChannelBuffer): MemcachedParameters = {
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
         case ReplaceIfUnmodifiedRequest => {
            index += 1
            getVersion(args(index))
         }
         case _ => -1
      }
      index += 1
      val noReply = parseNoReply(index, args)
      new MemcachedParameters(length, lifespan, -1, streamVersion, noReply, flags, "", 0)
   }

   override protected def readValue(b: ChannelBuffer) {
      b.readBytes(rawValue)
      skipLine(b) // read the rest of line to clear CRLF after value Byte[]
   }

   override def createValue(): Array[Byte] = rawValue

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
            case e: NumberFormatException => return -1 // Either unformatted number, or noreply found
         }
      }
      else 0
   }

   override def getCache: Cache[String, Array[Byte]] = cache

   override protected def customDecodeHeader(ch: Channel, buffer: ChannelBuffer): AnyRef = {
      header.op match {
         case FlushAllRequest => flushAll(buffer, ch, isReadParams = false) // Without params
         case VersionRequest => {
            val ret = new StringBuilder().append("VERSION ").append(Version.VERSION).append(CRLF)
            writeResponse(ch, ret)
         }
         case QuitRequest => closeChannel(ch)
      }
   }

   override protected def customDecodeKey(ch: Channel, buffer: ChannelBuffer): AnyRef = {
      header.op match {
         case AppendRequest | PrependRequest | IncrementRequest | DecrementRequest => {
            key = readKey(buffer)._1
            checkpointTo(DECODE_PARAMETERS)
         }
         case FlushAllRequest => flushAll(buffer, ch, isReadParams = true) // With params
      }
   }

   override protected def customDecodeValue(ch: Channel, buffer: ChannelBuffer): AnyRef = {
      val op = header.op
      op match {
         case AppendRequest | PrependRequest => {
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
         }
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
                  case IncrementRequest => {
                     val candidateCounter = prevCounter + delta
                     if (candidateCounter > MAX_UNSIGNED_LONG) 0 else candidateCounter
                  }
                  case DecrementRequest => {
                     val candidateCounter = prevCounter - delta
                     if (candidateCounter < 0) 0 else candidateCounter
                  }
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

   private def flushAll(b: ChannelBuffer, ch: Channel, isReadParams: Boolean): AnyRef = {
      if (isReadParams) readParameters(ch, b)
      val flushFunction = (cache: AdvancedCache[String, Array[Byte]]) =>
         cache.withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STORE).clear()
      val flushDelay = if (params == null) 0 else params.flushDelay
      if (flushDelay == 0)
         flushFunction(cache)
      else
         scheduler.schedule(new DelayedFlushAll(cache, flushFunction), toMillis(flushDelay), TimeUnit.MILLISECONDS)
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

   override def createSuccessResponse(prev: Array[Byte]): AnyRef = {
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

   override def createNotExecutedResponse(prev: Array[Byte]): AnyRef = {
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

   override def createNotExistResponse: AnyRef = {
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

   override def createGetResponse(k: String, entry: CacheEntry): AnyRef = {
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

   override def createMultiGetResponse(pairs: Map[String, CacheEntry]): AnyRef = {
      val elements = new ListBuffer[ChannelBuffer]
      val op = header.op
      op match {
         case GetRequest | GetWithVersionRequest => {
            for ((k, entry) <- pairs)
               elements += buildGetResponse(op, k, entry)
            elements += wrappedBuffer(END)
         }
      }
      elements.toArray
   }

   override def createErrorResponse(t: Throwable): AnyRef = {
      val sb = new StringBuilder
      t match {
         case m: MemcachedException => {
            m.getCause match {
               case u: UnknownOperationException => {
                  logExceptionReported(u)
                  ERROR
               }
               case c: ClosedChannelException => {
                  logExceptionReported(c)
                  null // no-op, only log
               }
               case i: IOException => {
                  logAndCreateErrorMessage(sb, m)
               }
               case n: NumberFormatException => {
                  logAndCreateErrorMessage(sb, m)
               }
               case i: IllegalStateException => {
                  logAndCreateErrorMessage(sb, m)
               }
               case _ => sb.append(m.getMessage).append(CRLF)
            }
         }
         case c: ClosedChannelException => {
            logExceptionReported(c)
            null // no-op, only log
         }
         case _ => sb.append(SERVER_ERROR).append(t.getMessage).append(CRLF)
      }
   }

   override protected def buildMetadata(): Metadata = {
      val version = new ServerEntryVersion(generateVersion(cache))
      MemcachedMetadata(params.flags, toMillis(params.lifespan), defaultMaxIdleTime, version)
   }

   private def logAndCreateErrorMessage(sb: StringBuilder, m: MemcachedException): StringBuilder = {
      logExceptionReported(m.getCause)
      sb.append(m.getMessage).append(CRLF)
   }

   override protected def createServerException(e: Exception, b: ChannelBuffer): (MemcachedException, Boolean) = {
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

   override def createStatsResponse: AnyRef = {
      val stats = cache.getAdvancedCache.getStats
      val sb = new StringBuilder
      Array[ChannelBuffer] (
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
         buildStat("bytes_read", transport.getTotalBytesRead, sb),
         buildStat("bytes_written", transport.getTotalBytesWritten, sb),
         buildStat("limit_maxbytes", 0, sb), // Unsupported
         buildStat("threads", 0, sb), // TODO: Through netty?
         buildStat("conn_yields", 0, sb), // Unsupported
         buildStat("reclaimed", 0, sb), // Unsupported
         wrappedBuffer(END)
      )
   }

   private def buildStat(stat: String, value: Any, sb: StringBuilder): ChannelBuffer = {
      sb.append("STAT").append(' ').append(stat).append(' ').append(value).append(CRLF)
      val buffer = wrappedBuffer(sb.toString.getBytes)
      sb.setLength(0)
      buffer
   }

   private def buildGetResponse(op: Enumeration#Value, k: String, entry: CacheEntry): ChannelBuffer = {
      val buf = buildGetHeaderBegin(k, entry, 0)
      writeGetHeaderData(entry.getValue.asInstanceOf[Array[Byte]], buf)
   }

   private def buildSingleGetResponse(k: String, entry: CacheEntry): ChannelBuffer = {
      val buf = buildGetHeaderBegin(k, entry, END_SIZE)
      writeGetHeaderData(entry.getValue.asInstanceOf[Array[Byte]], buf)
      writeGetHeaderEnd(buf)
   }
   
   private def buildGetHeaderBegin(k: String, entry: CacheEntry,
           extraSpace: Int): ChannelBuffer = {
      val data = entry.getValue.asInstanceOf[Array[Byte]]
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

   private def writeGetHeaderData(data: Array[Byte], buf: ChannelBuffer): ChannelBuffer = {
      buf.writeBytes(CRLFBytes)
      buf.writeBytes(data)
      buf.writeBytes(CRLFBytes)
      buf
   }

   private def writeGetHeaderEnd(buf: ChannelBuffer): ChannelBuffer = {
      buf.writeBytes(END)
      buf
   }

   private def buildSingleGetWithVersionResponse(k: String, entry: CacheEntry): ChannelBuffer = {
      val v = entry.getValue.asInstanceOf[Array[Byte]]
      // TODO: Would be nice for EntryVersion to allow retrieving the version itself...
      val version = entry.getMetadata.version().asInstanceOf[ServerEntryVersion].version.toString.getBytes
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

   private def numericLimitCheck(number: String, maxValue: Long, message: String): Long =  {
      val numeric = java.lang.Long.parseLong(number)
      if (java.lang.Long.parseLong(number) > maxValue)
         throw new NumberFormatException(message + " sent (" + number
            + ") exceeds the limit (" + maxValue + ")")
      numeric
   }
}

class MemcachedParameters(override val valueLength: Int, override val lifespan: Int,
                          override val maxIdle: Int, override val streamVersion: Long,
                          val noReply: Boolean, val flags: Long, val delta: String,
                          val flushDelay: Int)
        extends RequestParameters(valueLength, lifespan, maxIdle, streamVersion) {
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
   def toRequest(commandName: String, endOfOp: Boolean, buffer: ChannelBuffer): Enumeration#Value = {
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
         case _ => {
            if (!endOfOp) {
               val line = readDiscardedLine(buffer) // Read rest of line to clear the operation
               debug("Unexpected operation '%s', rest of line contains: %s",
                  commandName, line)
            }

            throw new UnknownOperationException("Unknown operation: " + commandName)
         }
      }
      op
   }
}

class MemcachedException(message: String, cause: Throwable) extends Exception(message, cause)
