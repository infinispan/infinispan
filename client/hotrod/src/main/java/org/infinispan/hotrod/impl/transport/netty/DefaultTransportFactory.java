package org.infinispan.hotrod.impl.transport.netty;

import java.util.concurrent.ExecutorService;

import org.infinispan.hotrod.configuration.TransportFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Default implementation of the {@link TransportFactory} interface which uses epoll if available and nio otherwise.
 */
public class DefaultTransportFactory implements TransportFactory {
   public Class<? extends SocketChannel> socketChannelClass() {
      return EPollAvailable.USE_NATIVE_EPOLL ? EpollSocketChannel.class : NioSocketChannel.class;
   }

   public EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      return EPollAvailable.USE_NATIVE_EPOLL ?
            new EpollEventLoopGroup(maxExecutors, executorService) :
            new NioEventLoopGroup(maxExecutors, executorService);
   }
}
