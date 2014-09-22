package org.infinispan.server.hotrod

/**
 * Hot Rod specific operations. Enumeration starts at a number other that 0 not to clash with common operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object HotRodOperation extends Enumeration(20) {
   type HotRodOperation = Value

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
}
