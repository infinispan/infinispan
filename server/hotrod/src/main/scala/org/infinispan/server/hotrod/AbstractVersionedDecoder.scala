package org.infinispan.server.hotrod

import org.infinispan.server.core.RequestParameters
import org.infinispan.server.core.CacheValue
import org.infinispan.server.core.transport.{ChannelBuffer}
import org.infinispan.Cache
import org.infinispan.stats.Stats
import org.infinispan.util.ByteArrayKey

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
   def readHeader(buffer: ChannelBuffer, messageId: Long): HotRodHeader

   /**
    * Read the key to operate on from the message.
    */
   def readKey(buffer: ChannelBuffer): ByteArrayKey

   /**
    * Read the parameters of the operation, if present.
    */
   def readParameters(header: HotRodHeader, buffer: ChannelBuffer): Option[RequestParameters]

   /**
    * Read the value part of the operation.
    */
   def createValue(params: RequestParameters, nextVersion: Long): CacheValue

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
   def createGetResponse(header: HotRodHeader, v: CacheValue, op: Enumeration#Value): AnyRef

   /**
    * Handle a protocol specific message.
    */
   def handleCustomRequest(header: HotRodHeader, buffer: ChannelBuffer, cache: Cache[ByteArrayKey, CacheValue]): AnyRef

   /**
    * Create a response for the stats command.
    */
   def createStatsResponse(header: HotRodHeader, stats: Stats): AnyRef

   /**
    * Create an error response based on the Throwable instance received.
    */
   def createErrorResponse(header: HotRodHeader, t: Throwable): ErrorResponse

   /**
    * Get an optimized cache instance depending on the operation parameters.
    */
   def getOptimizedCache(h: HotRodHeader, c: Cache[ByteArrayKey, CacheValue]): Cache[ByteArrayKey, CacheValue]
}