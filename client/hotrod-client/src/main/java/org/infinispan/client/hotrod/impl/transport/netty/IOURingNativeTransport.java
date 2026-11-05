package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.concurrent.ExecutorService;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.uring.IoUringIoHandler;

/**
 * @since 14.0
 **/
public class IOURingNativeTransport {

   public static Class<? extends SocketChannel> socketChannelClass() {
      return io.netty.channel.uring.IoUringSocketChannel.class;
   }

   public static EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      return new MultiThreadIoEventLoopGroup(maxExecutors, executorService, IoUringIoHandler.newFactory());
   }

   public static Class<? extends DatagramChannel> datagramChannelClass() {
      return io.netty.channel.uring.IoUringDatagramChannel.class;
   }

   public static boolean isAvailable() {
      return io.netty.channel.uring.IoUring.isAvailable();
   }

   public static String unavailableCause() {
      return io.netty.channel.uring.IoUring.unavailabilityCause().toString();
   }
}
