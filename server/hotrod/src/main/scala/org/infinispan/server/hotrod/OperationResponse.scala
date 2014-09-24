package org.infinispan.server.hotrod

import org.infinispan.server.core.Operation._
import org.infinispan.server.hotrod.HotRodOperation._
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

   def toResponse(request: Enumeration#Value): OperationResponse = {
      request match {
         case PutRequest => PutResponse
         case GetRequest => GetResponse
         case PutIfAbsentRequest => PutIfAbsentResponse
         case ReplaceRequest => ReplaceResponse
         case ReplaceIfUnmodifiedRequest => ReplaceIfUnmodifiedResponse
         case RemoveRequest => RemoveResponse
         case RemoveIfUnmodifiedRequest => RemoveIfUnmodifiedResponse
         case ContainsKeyRequest => ContainsKeyResponse
         case GetWithVersionRequest => GetWithVersionResponse
         case ClearRequest => ClearResponse
         case StatsRequest => StatsResponse
         case PingRequest => PingResponse
         case BulkGetRequest => BulkGetResponse
         case GetWithMetadataRequest => GetWithMetadataResponse
         case BulkGetKeysRequest => BulkGetKeysResponse
         case QueryRequest => QueryResponse
         case AuthMechListRequest => AuthMechListResponse
         case AuthRequest => AuthResponse
         case AddClientListenerRequest => AddClientListenerResponse
         case RemoveClientListenerRequest => RemoveClientListenerResponse
      }
   }

}

