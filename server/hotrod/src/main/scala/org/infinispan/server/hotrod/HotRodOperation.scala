package org.infinispan.server.hotrod

import org.infinispan.server.core.Operation._

/**
 * Hot Rod specific operations. Enumeration starts at a number other that 0 not to clash with common operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object HotRodOperation extends Enumeration(20) {
   type HotRodOperation = Value

   // NOTE: If adding new operation, make sure operation characteristic
   // methods below are updated accordingly!

   val RemoveIfUnmodifiedRequest = Value
   val ContainsKeyRequest = Value
   val ClearRequest = Value
   val QuitRequest = Value
   val PingRequest = Value
   val BulkGetRequest = Value
   val GetWithMetadataRequest = Value
   val BulkGetKeysRequest = Value
   val QueryRequest = Value
   val AuthMechListRequest = Value
   val AuthRequest = Value
   val AddClientListenerRequest = Value
   val RemoveClientListenerRequest = Value
   val SizeRequest = Value
   val ExecRequest = Value
   val PutAllRequest = Value
   val GetAllRequest = Value
   val IterationStartRequest = Value
   val IterationNextRequest = Value
   val IterationEndRequest = Value

   def canSkipIndexing(op: Enumeration#Value): Boolean = {
      op match {
         case PutRequest
              | RemoveRequest
              | PutIfAbsentRequest
              | RemoveIfUnmodifiedRequest
              | ReplaceRequest
              | ReplaceIfUnmodifiedRequest
              | PutAllRequest => true
         case _ => false
      }
   }

   def canSkipCacheLoading(op: Enumeration#Value): Boolean = {
      op match {
         case PutRequest
              | GetRequest
              | GetWithVersionRequest
              | RemoveRequest
              | ContainsKeyRequest
              | BulkGetRequest
              | GetWithMetadataRequest
              | BulkGetKeysRequest
              | PutAllRequest => true
         case _ => false
      }
   }

   def isNotConditionalAndCanReturnPrevious(op: Enumeration#Value): Boolean = {
      op match {
         case PutRequest => true
         case _ => false
      }
   }

   def canReturnPreviousValue(op: Enumeration#Value): Boolean = {
      op match {
         case PutRequest
              | RemoveRequest
              | PutIfAbsentRequest
              | ReplaceRequest
              | ReplaceIfUnmodifiedRequest
              | RemoveIfUnmodifiedRequest => true
         case _ => false
      }
   }

   def isConditional(op: Enumeration#Value): Boolean = {
      op match {
         case PutIfAbsentRequest
              | ReplaceRequest
              | ReplaceIfUnmodifiedRequest
              | RemoveIfUnmodifiedRequest => true
         case _ => false
      }
   }

}
