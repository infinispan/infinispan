package org.infinispan.server.core.transport;

import static org.infinispan.server.core.logging.Log.SERVER;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

// This is a separate class for easier replacement within Quarkus
public final class NativeTransport {
   private static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().startsWith("linux");
   private static final String USE_EPOLL_PROPERTY = "infinispan.server.channel.epoll";
   private static final String USE_IOURING_PROPERTY = "infinispan.server.channel.iouring";

   private static final boolean EPOLL_DISABLED = System.getProperty(USE_EPOLL_PROPERTY, "true").equalsIgnoreCase("false");
   private static final boolean IOURING_DISABLED = System.getProperty(USE_IOURING_PROPERTY, "true").equalsIgnoreCase("false");

   // Has to be after other static variables to ensure they are initialized
   private static final Type TYPE = transportType();

   enum Type {
      EPOLL,
      IOURING,
      NIO
   }

   private static Type transportType() {
      if (useNativeEpoll()) {
         return Type.EPOLL;
      } else if (useNativeIOUring()) {
         return Type.IOURING;
      } else {
         return Type.NIO;
      }
   }

   private static boolean useNativeEpoll() {
      try {
         Class.forName("io.netty.channel.epoll.Epoll", true, NativeTransport.class.getClassLoader());
         if (Epoll.isAvailable()) {
            return !EPOLL_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               SERVER.epollNotAvailable(Epoll.unavailabilityCause().toString());
            }
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            SERVER.epollNotAvailable(e.getMessage());
         }
      }
      return false;
   }

   private static boolean useNativeIOUring() {
      try {
         Class.forName("io.netty.incubator.channel.uring.IOUring", true, NativeTransport.class.getClassLoader());
         if (IOURingNativeTransport.isAvailable()) {
            return !IOURING_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               SERVER.ioUringNotAvailable(IOURingNativeTransport.unavailableCause());
            }
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            SERVER.ioUringNotAvailable(e.getMessage());
         }
      }
      return false;
   }

   public static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
      switch (TYPE) {
         case EPOLL -> {
            SERVER.usingTransport("Epoll");
            return EpollServerSocketChannel.class;
         }
         case IOURING ->  {
            SERVER.usingTransport("IOURING");
            return IOURingNativeTransport.serverSocketChannelClass();
         }
         default ->  {
            SERVER.usingTransport("NIO");
            return NioServerSocketChannel.class;
         }
      }
   }

   public static MultithreadEventLoopGroup createEventLoopGroup(int maxExecutors, ThreadFactory threadFactory) {
      return switch (TYPE) {
         case EPOLL -> new EpollEventLoopGroup(maxExecutors, threadFactory);
         case IOURING -> IOURingNativeTransport.createEventLoopGroup(maxExecutors, threadFactory);
         default -> new NioEventLoopGroup(maxExecutors, threadFactory);
      };
   }
}
