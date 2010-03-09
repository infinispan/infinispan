package org.infinispan.server.hotrod

import org.infinispan.context.Flag
   
/**
 * // TODO: Document this
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class Command(val cacheName: String,
                       val id: Long,
                       val flags: Set[Flag]) {

   def perform(cache: Cache): Response
   
}