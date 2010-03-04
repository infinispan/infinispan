package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.ChannelHandlerContext
import org.infinispan.server.core.{MessageEvent, CommandHandler}

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
      }


//      e.getMessage match {
//         case s: StorageCommand => s.perform(s)
//      }
   }
}