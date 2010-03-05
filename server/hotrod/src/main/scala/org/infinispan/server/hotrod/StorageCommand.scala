package org.infinispan.server.hotrod

import org.infinispan.context.Flag

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class StorageCommand(override val cacheName: String,
                     override val id: Long,
                     val key: Array[Byte],
                     val lifespan: Int,
                     val maxIdle: Int,
                     val value: Array[Byte],
                     val flags: Set[Flag])
                    (val op: (Cache, StorageCommand) => Response) extends Command(cacheName, id) {

   override def perform(cache: Cache): Response = {
      op(cache, this)
   }

   override def toString = {
      new StringBuilder().append("StorageCommand").append("{")
         .append("cacheName=").append(cacheName)
         .append(", id=").append(id)
         .append(", key=").append(key)
         .append(", lifespan=").append(lifespan)
         .append(", maxIdle=").append(maxIdle)
         .append(", value=").append(value)
         .append(", flags=").append(flags)
         .append("}").toString
   }
}