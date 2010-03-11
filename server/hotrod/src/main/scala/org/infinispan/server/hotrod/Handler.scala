package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.{CommandHandler, MessageEvent, ChannelHandlerContext}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class Handler(val hotCache: CallerCache) extends CommandHandler {

   override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      e.getMessage match {
//         case c: StorageCommand => e.getChannel.write(c.op(hotCache, c))
         case c: Command => e.getChannel.write(c.perform(hotCache))
         case er: ErrorResponse => e.getChannel.write(er) 
      }


//      e.getMessage match {
//         case s: StorageCommand => s.perform(s)
//      }
   }
}