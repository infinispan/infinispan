package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class RetrievalCommand(override val cacheName: String,
                       override val id: Long,
                       val key: Array[Byte])
                      (val op: (Cache, RetrievalCommand) => Response) extends Command(cacheName, id) {

   override def perform(cache: Cache): Response = {
      op(cache, this)
   }

}