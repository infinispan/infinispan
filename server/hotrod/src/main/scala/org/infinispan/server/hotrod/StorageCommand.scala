package org.infinispan.server.hotrod

import org.infinispan.context.Flag

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.0
 */

// val cache: Cache[Array[Byte], Array[Byte]]
class StorageCommand(val cacheName: String, val key: Array[Byte], val lifespan: Int,
                     val maxIdle: Int, val value: Array[Byte], val flags: Set[Flag])
                    (val op: (Cache, StorageCommand) => Reply.Value)
//{
//
////   def perform(op: StorageCommand => Reply.Value) {
////      op(this)
////   }
////
//}