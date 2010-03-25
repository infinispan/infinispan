package org.infinispan.server.memcached

import org.infinispan.server.core.Operation._
import org.infinispan.server.memcached.MemcachedOperation._
import org.infinispan.server.core.Logging

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// todo: maybe try to abstract this into something that can be shared betwen hr and memcached
// todo(2): Do I need this at all? Simply move it to decoder!
object OperationResolver extends Logging {
   // TODO: Rather than holding a map, check if the String could be passed as part of the Enumeration and whether this could be retrieved in a O(1) op
   private val operations = Map[String, Enumeration#Value](
      "set" -> PutRequest,
      "add" -> PutIfAbsentRequest,
      "replace" -> ReplaceRequest,
      "cas" -> ReplaceIfUnmodifiedRequest,
      "append" -> AppendRequest,
      "prepend" -> PrependRequest,
      "get" -> GetRequest,
      "gets" -> GetWithVersionRequest,
      "delete" -> RemoveRequest,
      "incr" -> IncrementRequest,
      "decr" -> DecrementRequest,
      "flush_all" -> FlushAllRequest,
      "version" -> VersionRequest,
      "stats" -> StatsRequest
   )

   def resolve(commandName: String): Option[Enumeration#Value] = {
      trace("Operation: {0}", commandName)
      val op = operations.get(commandName)
      op
   }
}

