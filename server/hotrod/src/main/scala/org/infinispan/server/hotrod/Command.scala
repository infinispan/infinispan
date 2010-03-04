package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class Command(val cacheName: String,
                       val id: Long)
//   type AnyCommand <: Command
//
{
  def perform(cache: Cache): Response
}
//
//}
////{
//////   type AnyCommand <: Command
//////   def perform(op: Unit => Replies)
////}