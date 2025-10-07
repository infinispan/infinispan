package org.infinispan.server.core.transport;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;

/**
 * @since 14.0
 **/
public class IOURingNativeTransport {

   public static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
      // return io.netty.channel.uring.IoUringServerSocketChannel.class;
      return io.netty.incubator.channel.uring.IOUringServerSocketChannel.class;
   }

   public static MultithreadEventLoopGroup createEventLoopGroup(int maxExecutors, ThreadFactory threadFactory) {
      // new MultiThreadIoEventLoopGroup(maxExecutors, threadFactory, IoUringIoHandler.newFactory(maxExecutors));
      return new IOUringEventLoopGroup(maxExecutors, threadFactory);
   }

   public static boolean isAvailable() {
      // io.netty.channel.uring.IoUring.isAvailable();
      return io.netty.incubator.channel.uring.IOUring.isAvailable();
   }

   public static String unavailableCause() {
      // io.netty.channel.uring.IoUring.unavailabilityCause().toString()
      return io.netty.incubator.channel.uring.IOUring.unavailabilityCause().toString();
   }
}
