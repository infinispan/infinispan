package org.infinispan.server.core.transport;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;

/**
 * @since 14.0
 **/
public class IOURingNativeTransport {

   public static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
      return IOUringServerSocketChannel.class;
   }

   public static MultithreadEventLoopGroup createEventLoopGroup(int maxExecutors, ThreadFactory threadFactory) {
      return new IOUringEventLoopGroup(maxExecutors, threadFactory);
   }
}
