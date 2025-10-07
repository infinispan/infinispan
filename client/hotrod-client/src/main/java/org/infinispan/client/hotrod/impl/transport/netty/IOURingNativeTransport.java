package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.concurrent.ExecutorService;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;

/**
 * @since 14.0
 **/
public class IOURingNativeTransport {

   public static Class<? extends SocketChannel> socketChannelClass() {
      // return io.netty.channel.uring.IoUringSocketChannel.class;
      return io.netty.incubator.channel.uring.IOUringSocketChannel.class;
   }

   public static EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      // new MultiThreadIoEventLoopGroup(maxExecutors, executorService, IoUringIoHandler.newFactory(maxExecutors));
      return new IOUringEventLoopGroup(maxExecutors, executorService);
   }

   public static Class<? extends DatagramChannel> datagramChannelClass() {
      // return io.netty.channel.uring.IoUringDatagramChannel.class;
      return io.netty.incubator.channel.uring.IOUringDatagramChannel.class;
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
