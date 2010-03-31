package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel.{ChannelFuture => NettyChannelFuture}
import org.infinispan.server.core.transport.{Channel, ChannelFuture}
import java.util.concurrent.TimeUnit

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class ChannelFutureAdapter(val future: NettyChannelFuture) extends ChannelFuture {
   
   override def getChannel: Channel = new ChannelAdapter(future.getChannel())

   override def isDone: Boolean = future.isDone

   override def isCancelled: Boolean = future.isCancelled

   override def setSuccess: Boolean = future.setSuccess

   override def setFailure(cause: Throwable): Boolean = future.setFailure(cause)

   override def await: ChannelFuture = {
      future.await
      this
   }

   override def awaitUninterruptibly: ChannelFuture = {
      future.awaitUninterruptibly
      this
   }

   override def await(timeout: Long, unit: TimeUnit): Boolean = future.await(timeout, unit)

   override def await(timeoutMillis: Long): Boolean = future.await(timeoutMillis)
   
   override def awaitUninterruptibly(timeout: Long, unit: TimeUnit): Boolean = future.awaitUninterruptibly(timeout, unit)
   
   override def awaitUninterruptibly(timeoutMillis: Long): Boolean = future.awaitUninterruptibly(timeoutMillis)
   
}