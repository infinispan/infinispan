package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

abstract class Cache {
   def put(c: StorageCommand): Reply.Value
   def get(c: RetrievalCommand): Reply.Value
}