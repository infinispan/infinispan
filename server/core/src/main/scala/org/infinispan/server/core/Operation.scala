package org.infinispan.server.core

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
object Operation extends Enumeration {
   type Operation = Value
   val PutRequest, PutIfAbsentRequest, ReplaceRequest, ReplaceIfUnmodifiedRequest = Value
   val GetRequest, GetWithVersionRequest = Value
   val RemoveRequest, StatsRequest = Value
}