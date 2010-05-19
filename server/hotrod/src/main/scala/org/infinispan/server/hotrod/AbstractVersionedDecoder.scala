package org.infinispan.server.hotrod

import org.infinispan.server.core.RequestParameters
import org.infinispan.server.core.CacheValue
import org.infinispan.server.core.transport.{ChannelBuffer}
import org.infinispan.Cache
import org.infinispan.stats.Stats

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */   
abstract class AbstractVersionedDecoder {

   def readHeader(buffer: ChannelBuffer, messageId: Long): HotRodHeader

   def readKey(buffer: ChannelBuffer): CacheKey

   def readKeys(buffer: ChannelBuffer): Array[CacheKey]

   def readParameters(header: HotRodHeader, buffer: ChannelBuffer): Option[RequestParameters]

   def createValue(params: RequestParameters, nextVersion: Long): CacheValue

   def createSuccessResponse(header: HotRodHeader, prev: CacheValue): AnyRef

   def createNotExecutedResponse(header: HotRodHeader, prev: CacheValue): AnyRef

   def createNotExistResponse(header: HotRodHeader): AnyRef

   def createGetResponse(header: HotRodHeader, v: CacheValue, op: Enumeration#Value): AnyRef

   def handleCustomRequest(header: HotRodHeader, buffer: ChannelBuffer, cache: Cache[CacheKey, CacheValue]): AnyRef

   def createStatsResponse(header: HotRodHeader, stats: Stats): AnyRef

   def createErrorResponse(header: HotRodHeader, t: Throwable): AnyRef

   def getOptimizedCache(h: HotRodHeader, c: Cache[CacheKey, CacheValue]): Cache[CacheKey, CacheValue]
}