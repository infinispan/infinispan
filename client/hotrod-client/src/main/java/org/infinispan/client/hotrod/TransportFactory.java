package org.infinispan.client.hotrod;

import java.util.concurrent.ExecutorService;

import org.infinispan.client.hotrod.impl.transport.netty.DefaultTransportFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

/**
 * TransportFactory is responsible for creating Netty's {@link SocketChannel}s and {@link EventLoopGroup}s.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface TransportFactory {
   TransportFactory DEFAULT = new DefaultTransportFactory();

   /**
    * Returns the Netty {@link SocketChannel} class to use in the transport.
    */
   Class<? extends SocketChannel> socketChannelClass();

   /**
    * Creates an event loop group
    *
    * @param maxExecutors the maximum number of executors
    * @param executorService the executor service to use
    * @return an instance of Netty's {@link EventLoopGroup}
    */
   EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService);
}
