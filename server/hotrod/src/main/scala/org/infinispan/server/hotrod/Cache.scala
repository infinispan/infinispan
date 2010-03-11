package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

abstract class Cache {
   def put(c: StorageCommand): Response
   def get(c: RetrievalCommand): Response
   def putIfAbsent(c: StorageCommand): Response
}