package org.infinispan.server.hotrod

import org.infinispan.context.Flag

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class StorageCommand(override val cacheName: String,
                     override val id: Long,
                     val k: Key,
                     val lifespan: Int,
                     val maxIdle: Int,
                     val v: Value,
                     override val flags: Set[Flag])
                    (val op: (Cache, StorageCommand) => Response) extends Command(cacheName, id, flags) {

   override def perform(cache: Cache): Response = {
      op(cache, this)
   }

   override def toString = {
      new StringBuilder().append("StorageCommand").append("{")
         .append("cacheName=").append(cacheName)
         .append(", id=").append(id)
         .append(", k=").append(k)
         .append(", lifespan=").append(lifespan)
         .append(", maxIdle=").append(maxIdle)
         .append(", v=").append(v)
         .append(", flags=").append(flags)
         .append("}").toString
   }
}