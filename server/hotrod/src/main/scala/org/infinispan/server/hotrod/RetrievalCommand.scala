package org.infinispan.server.hotrod

import org.infinispan.context.Flag

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class RetrievalCommand(override val cacheName: String,
                       override val id: Long,
                       val k: Key,
                       override val flags: Set[Flag])
                      (val op: (Cache, RetrievalCommand) => Response) extends Command(cacheName, id, flags) {

   override def perform(cache: Cache): Response = {
      op(cache, this)
   }

}