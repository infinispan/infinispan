package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.ExecutorService;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

// This is a separate class for easier replacement within Quarkus
public final class NativeTransport {
   private static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().startsWith("linux");
   private static final String USE_EPOLL_PROPERTY = "infinispan.server.channel.epoll";
   private static final String USE_IOURING_PROPERTY = "infinispan.server.channel.iouring";

   private static final boolean EPOLL_DISABLED = System.getProperty(USE_EPOLL_PROPERTY, "true").equalsIgnoreCase("false");
   private static final boolean IOURING_DISABLED = System.getProperty(USE_IOURING_PROPERTY, "true").equalsIgnoreCase("false");

   // Has to be after other static variables to ensure they are initialized
   static final boolean USE_NATIVE_EPOLL = useNativeEpoll();
   static final boolean USE_NATIVE_IOURING = useNativeIOUring();

   private static boolean useNativeEpoll() {
      try {
         Class.forName("io.netty.channel.epoll.Epoll", true, NativeTransport.class.getClassLoader());
         if (Epoll.isAvailable()) {
            return !EPOLL_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               HOTROD.epollNotAvailable(Epoll.unavailabilityCause().toString());
            }
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            HOTROD.epollNotAvailable(e.getMessage());
         }
      }
      return false;
   }

   private static boolean useNativeIOUring() {
      try {
         Class.forName("io.netty.incubator.channel.uring.IOUring", true, NativeTransport.class.getClassLoader());
         if (io.netty.channel.uring.IoUring.isAvailable()) {
            return !IOURING_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               HOTROD.ioUringNotAvailable(io.netty.channel.uring.IoUring.unavailabilityCause().toString());
            }
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            HOTROD.ioUringNotAvailable(e.getMessage());
         }
      }
      return false;
   }

   public static Class<? extends SocketChannel> socketChannelClass() {
      if (USE_NATIVE_EPOLL) {
         return EpollSocketChannel.class;
      } else if (USE_NATIVE_IOURING) {
         return IoURingNativeTransport.socketChannelClass();
      } else {
         return NioSocketChannel.class;
      }
   }

   public static Class<? extends DatagramChannel> datagramChannelClass() {
      if (USE_NATIVE_EPOLL) {
         return EpollDatagramChannel.class;
      } else if (USE_NATIVE_IOURING) {
         return IoURingNativeTransport.datagramChannelClass();
      } else {
         return NioDatagramChannel.class;
      }
   }

   public static EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
      if (USE_NATIVE_EPOLL) {
         return new MultiThreadIoEventLoopGroup(maxExecutors, executorService, EpollIoHandler.newFactory());
      } else if (USE_NATIVE_IOURING) {
         return IoURingNativeTransport.createEventLoopGroup(maxExecutors, executorService);
      } else {
         return new MultiThreadIoEventLoopGroup(maxExecutors, executorService, NioIoHandler.newFactory(SelectorProvider.provider()));
      }
   }
}
