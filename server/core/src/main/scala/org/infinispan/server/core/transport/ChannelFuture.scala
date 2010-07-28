package org.infinispan.server.core.transport

import java.util.concurrent.TimeUnit

/**
 * A channel future representing the result of a channel operation.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class ChannelFuture {
   def getChannel: Channel
   def isDone: Boolean
   def isCancelled: Boolean
   def setSuccess: Boolean
   def setFailure(cause: Throwable): Boolean
   def await: ChannelFuture
   def awaitUninterruptibly: ChannelFuture
   def await(timeout: Long, unit: TimeUnit): Boolean
   def await(timeoutMillis: Long): Boolean
   def awaitUninterruptibly(timeout: Long, unit: TimeUnit): Boolean
   def awaitUninterruptibly(timeoutMillis: Long): Boolean
}