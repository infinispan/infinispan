package org.infinispan.server.hotrod

import org.infinispan.server.core.RequestParameters
import org.infinispan.server.core.CacheValue
import org.infinispan.server.core.transport.{ChannelBuffers, Channel, ChannelBuffer}
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

   def sendPutResponse(messageId: Long): AnyRef

   def sendGetResponse(messageId: Long, v: CacheValue, op: Enumeration#Value): AnyRef

   def sendPutIfAbsentResponse(messageId: Long, prev: CacheValue): AnyRef

   def sendReplaceResponse(messageId: Long, prev: CacheValue): AnyRef

   def sendReplaceIfUnmodifiedResponse(messageId: Long, v: Option[CacheValue], prev: Option[CacheValue]): AnyRef

   def sendRemoveResponse(messageId: Long, prev: CacheValue): AnyRef

   def handleCustomRequest(header: HotRodHeader, buffer: ChannelBuffer, cache: Cache[CacheKey, CacheValue]): AnyRef

   def sendStatsResponse(header: HotRodHeader, stats: Stats): AnyRef

}