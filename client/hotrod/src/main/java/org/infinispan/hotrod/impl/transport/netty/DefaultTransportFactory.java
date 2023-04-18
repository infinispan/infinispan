package org.infinispan.hotrod.impl.transport.netty;

import java.util.concurrent.ExecutorService;

import org.infinispan.hotrod.configuration.TransportFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;

/**
 * Default implementation of the {@link TransportFactory} interface which uses epoll if available and nio otherwise.
 */
public class DefaultTransportFactory implements TransportFactory {
   public Class<? extends SocketChannel> socketChannelClass() {
      return NativeTransport.socketChannelClass();
   }

   public EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      return NativeTransport.createEventLoopGroup(maxExecutors, executorService);
   }

   @Override
   public Class<? extends DatagramChannel> datagramChannelClass() {
      return NativeTransport.datagramChannelClass();
   }
}
