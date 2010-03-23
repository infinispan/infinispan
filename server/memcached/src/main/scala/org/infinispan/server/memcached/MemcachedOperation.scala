package org.infinispan.server.memcached

import org.infinispan.server.core.Operation._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

object MemcachedOperation extends Enumeration(10) {
   type MemcachedOperation = Value
   val AppendRequest, PrependRequest = Value
   val IncrementRequest, DecrementRequest = Value
   val FlushAllRequest, VersionRequest = Value
}