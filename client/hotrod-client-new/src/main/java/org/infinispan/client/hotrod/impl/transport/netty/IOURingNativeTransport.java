package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.concurrent.ExecutorService;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.incubator.channel.uring.IOUringDatagramChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringSocketChannel;

/**
 * @since 14.0
 **/
public class IOURingNativeTransport {

   public static Class<? extends SocketChannel> socketChannelClass() {
      return IOUringSocketChannel.class;
   }

   public static EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      return new IOUringEventLoopGroup(maxExecutors, executorService);
   }

   public static Class<? extends DatagramChannel> datagramChannelClass() {
      return IOUringDatagramChannel.class;
   }
}
