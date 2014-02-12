package org.infinispan.server.hotrod

import org.infinispan.AdvancedCache
import org.infinispan.stats.Stats
import org.infinispan.server.core.{QueryFacade, RequestParameters}
import org.infinispan.server.core.transport.NettyTransport
import org.infinispan.container.entries.CacheEntry
import io.netty.buffer.ByteBuf

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
   def readHeader(buffer: ByteBuf, version: Byte, messageId: Long, header: HotRodHeader): Boolean

   /**
    * Read the key to operate on from the message.
    */
   def readKey(header: HotRodHeader, buffer: ByteBuf): (Array[Byte], Boolean)

   /**
    * Read the parameters of the operation, if present.
    */
   def readParameters(header: HotRodHeader, buffer: ByteBuf): (RequestParameters, Boolean)

   /**
    * Create a successful response.
    */
   def createSuccessResponse(header: HotRodHeader, prev: Array[Byte]): AnyRef

   /**
    * Create a response indicating the the operation could not be executed.
    */
   def createNotExecutedResponse(header: HotRodHeader, prev: Array[Byte]): AnyRef

   /**
    * Create a response indicating that the key, which the message tried to operate on, did not exist.
    */
   def createNotExistResponse(header: HotRodHeader): AnyRef

   /**
    * Create a response for get a request.
    */
   def createGetResponse(header: HotRodHeader, entry: CacheEntry): AnyRef

   /**
    * Handle a protocol specific header reading.
    */
   def customReadHeader(header: HotRodHeader, buffer: ByteBuf, cache: AdvancedCache[Array[Byte], Array[Byte]]): AnyRef

   /**
    * Handle a protocol specific key reading.
    */
   def customReadKey(header: HotRodHeader, buffer: ByteBuf, cache: AdvancedCache[Array[Byte], Array[Byte]],
           queryFacades: Seq[QueryFacade]): AnyRef

   /**
    * Handle a protocol specific value reading.
    */
   def customReadValue(header: HotRodHeader, buffer: ByteBuf, cache: AdvancedCache[Array[Byte], Array[Byte]]): AnyRef

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
   def getOptimizedCache(h: HotRodHeader, c: AdvancedCache[Array[Byte], Array[Byte]]): AdvancedCache[Array[Byte], Array[Byte]]

}
