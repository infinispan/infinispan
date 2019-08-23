package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.concurrent.ExecutorService;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

class TransportHelper {
   static Class<? extends SocketChannel> socketChannel() {
      return EPollAvailable.USE_NATIVE_EPOLL ? EpollSocketChannel.class : NioSocketChannel.class;
   }

   static EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      return EPollAvailable.USE_NATIVE_EPOLL ?
            new EpollEventLoopGroup(maxExecutors, executorService) :
            new NioEventLoopGroup(maxExecutors, executorService);
   }
}
