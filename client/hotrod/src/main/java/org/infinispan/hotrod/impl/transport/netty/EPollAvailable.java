package org.infinispan.hotrod.impl.transport.netty;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import io.netty.channel.epoll.Epoll;

// This is a separate class for better replacement within Quarkus as it doesn't support native EPoll
final class EPollAvailable {

   private static final String USE_EPOLL_PROPERTY = "infinispan.server.channel.epoll";
   private static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().startsWith("linux");
   private static final boolean EPOLL_DISABLED = System.getProperty(USE_EPOLL_PROPERTY, "true").equalsIgnoreCase("false");

   // Has to be after other static variables to ensure they are initialized
   static final boolean USE_NATIVE_EPOLL = useNativeEpoll();

   private static boolean useNativeEpoll() {
      try {
         Class.forName("io.netty.channel.epoll.Epoll", true, EPollAvailable.class.getClassLoader());
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
}
