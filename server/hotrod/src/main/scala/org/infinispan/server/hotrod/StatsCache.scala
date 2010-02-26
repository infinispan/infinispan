package org.infinispan.server.hotrod

/**
 * // TODO: Document
 *
 * The idea with this trait is to be able to do stuff like this:
 *
 * If stats enabled:
 * val cache = (new CallerCache with StatsCache)
 * If stats disabled:
 * val cache = new CallerCache
 *
 * A very easy way to define interceptors!!!!
 *
 * @author Galder Zamarreño
 * @since 4.1
 */

trait StatsCache extends Cache {

   abstract override def put(c: StorageCommand): Reply.Value = {
      // TODO: calculate stats if necessary
      super.put(c)
   }

   abstract override def get(c: RetrievalCommand): Reply.Value = {
      // TODO: calculate stats if necessary
      super.get(c)
   }
   
}