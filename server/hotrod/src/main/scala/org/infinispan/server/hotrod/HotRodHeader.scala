package org.infinispan.server.hotrod

import org.infinispan.server.core.RequestHeader
import org.infinispan.server.hotrod.ProtocolFlag._
import org.infinispan.server.hotrod.OperationResponse._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class HotRodHeader(override val op: Enumeration#Value, val messageId: Long, val cacheName: String,
                   val flag: ProtocolFlag, val clientIntelligence: Short, val topologyId: Int,
                   val decoder: AbstractVersionedDecoder) extends RequestHeader(op) {

   // TODO: add meaningfull toString()
}

class ErrorHeader(override val messageId: Long) extends HotRodHeader(ErrorResponse, messageId, "", NoFlag, 0, 0, null) {
   // TODO: add meaningfull toString()   
}