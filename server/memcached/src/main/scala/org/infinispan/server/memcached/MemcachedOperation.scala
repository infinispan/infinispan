package org.infinispan.server.memcached

/**
 * Memcached operations. The enumeration stats at a number other than 0 to make sure it does not clash with common operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object MemcachedOperation extends Enumeration(10) {
   type MemcachedOperation = Value
   val AppendRequest, PrependRequest = Value
   val IncrementRequest, DecrementRequest = Value
   val FlushAllRequest, VersionRequest = Value
}