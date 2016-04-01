package org.infinispan.server.hotrod

import org.infinispan.server.core.Operation._
import scala.annotation.switch

/**
 * @author Galder Zamarreño
 */
object OperationResponse extends Enumeration {
   type OperationResponse = Enumeration#Value
   val PutResponse = Value(0x02)
   val GetResponse = Value(0x04)
   val PutIfAbsentResponse = Value(0x06)
   val ReplaceResponse = Value(0x08)
   val ReplaceIfUnmodifiedResponse = Value(0x0A)
   val RemoveResponse = Value(0x0C)
   val RemoveIfUnmodifiedResponse = Value(0x0E)
   val ContainsKeyResponse = Value(0x10)
   val GetWithVersionResponse = Value(0x12)
   val ClearResponse = Value(0x14)
   val StatsResponse = Value(0x16)
   val PingResponse = Value(0x18)
   val BulkGetResponse = Value(0x1A)
   val ErrorResponse = Value(0x50)

   // 1.2
   val GetWithMetadataResponse = Value(0x1C)
   val BulkGetKeysResponse = Value(0x1E)

   // 1.3
   val QueryResponse = Value(0x20)

   // 2.0
   val AuthMechListResponse = Value(0x22)
   val AuthResponse = Value(0x24)
   val AddClientListenerResponse = Value(0x26)
   val RemoveClientListenerResponse = Value(0x28)
   val SizeResponse = Value(0x2A)
   val CacheEntryCreatedEventResponse = Value(0x60)
   val CacheEntryModifiedEventResponse = Value(0x61)
   val CacheEntryRemovedEventResponse = Value(0x62)
   val CacheEntryExpiredEventResponse = Value(0x63)
   
   // 2.1
   val ExecResponse = Value(0x2C)
   val PutAllResponse = Value(0x2E)
   val GetAllResponse = Value(0x30)

   // 2.3
   val IterationStartResponse = Value(0x32)
   val IterationNextResponse = Value(0x34)
   val IterationEndResponse = Value(0x36)

   def toResponse(request: HotRodOperation): OperationResponse = {
      // Go to java so switch case will be optimized properly
      OperationResponseJava.operationToResponse(request).asInstanceOf[OperationResponse]
   }

}

