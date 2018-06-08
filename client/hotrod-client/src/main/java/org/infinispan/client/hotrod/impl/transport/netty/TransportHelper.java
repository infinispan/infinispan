package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.concurrent.ExecutorService;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

class TransportHelper {
   private static Log log = LogFactory.getLog(TransportHelper.class);

   private static final String USE_EPOLL_PROPERTY = "infinispan.server.channel.epoll";
   private static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().startsWith("linux");
   private static final boolean EPOLL_DISABLED = System.getProperty(USE_EPOLL_PROPERTY, "true").equalsIgnoreCase("false");
   private static final boolean USE_NATIVE_EPOLL;

   static {
      boolean epoll;
      try {
         Class.forName("io.netty.channel.epoll.Epoll", true, TransportHelper.class.getClassLoader());
         if (Epoll.isAvailable()) {
            epoll = !EPOLL_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               log.epollNotAvailable(Epoll.unavailabilityCause().toString());
            }
            epoll = false;
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            log.epollNotAvailable(e.getMessage());
         }
         epoll = false;
      }

      USE_NATIVE_EPOLL = epoll;
   }

   static Class<? extends SocketChannel> socketChannel() {
      return USE_NATIVE_EPOLL ? EpollSocketChannel.class : NioSocketChannel.class;
   }

   static EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      return USE_NATIVE_EPOLL ?
            new EpollEventLoopGroup(maxExecutors, executorService) :
            new NioEventLoopGroup(maxExecutors, executorService);
   }
}
