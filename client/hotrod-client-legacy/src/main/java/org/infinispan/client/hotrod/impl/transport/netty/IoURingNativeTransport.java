package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.concurrent.ExecutorService;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.uring.IoUringDatagramChannel;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringSocketChannel;

/**
 * @since 14.0
 **/
public class IOURingNativeTransport {

   public static Class<? extends SocketChannel> socketChannelClass() {
      return IoUringSocketChannel.class;
   }

   public static EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      return new MultiThreadIoEventLoopGroup(maxExecutors, executorService, IoUringIoHandler.newFactory(maxExecutors));
   }

   public static Class<? extends DatagramChannel> datagramChannelClass() {
      return IoUringDatagramChannel.class;
   }
}
