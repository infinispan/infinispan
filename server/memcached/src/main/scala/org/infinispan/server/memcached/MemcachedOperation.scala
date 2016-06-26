package org.infinispan.server.memcached

/**
 * Memcached operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object MemcachedOperation extends Enumeration {
   type MemcachedOperation = Value
   val PutRequest, PutIfAbsentRequest, ReplaceRequest, ReplaceIfUnmodifiedRequest = Value
   val GetRequest, GetWithVersionRequest = Value
   val RemoveRequest, StatsRequest = Value
   val AppendRequest, PrependRequest = Value
   val IncrementRequest, DecrementRequest = Value
   val FlushAllRequest, VersionRequest = Value
   val VerbosityRequest, QuitRequest = Value
}