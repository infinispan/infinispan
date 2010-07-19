package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

object HotRodOperation extends Enumeration(20) {
   type HotRodOperation = Value

   val RemoveIfUnmodifiedRequest = Value
   val ContainsKeyRequest = Value
   val ClearRequest = Value
   val QuitRequest = Value
   val PingRequest = Value
   val BulkGetRequest = Value

}