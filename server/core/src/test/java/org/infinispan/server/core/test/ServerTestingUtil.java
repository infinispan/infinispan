package org.infinispan.server.core.test;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.function.Function;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.AbstractProtocolServer;

import io.netty.channel.unix.Errors;

/**
 * Infinispan servers testing util
 *
 * @author Galder Zamarreño
 * @author wburns
 */
public class ServerTestingUtil {

   private static final int DEFAULT_PORT = 15232;

   private static Log LOG = LogFactory.getLog(ServerTestingUtil.class);

   public static void killServer(AbstractProtocolServer<?> server) {
      try {
         if (server != null) server.stop();
      } catch (Throwable t) {
         LOG.error("Error stopping server", t);
      }
   }

   private static boolean isBindException(Throwable e) {
      if (e instanceof BindException)
         return true;
      if (e instanceof Errors.NativeIoException) {
         Errors.NativeIoException nativeIoException = (Errors.NativeIoException) e;
         return nativeIoException.getMessage().contains("bind");
      }
      return false;
   }

   public static int findFreePort() {
      try {
         try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
         }
      } catch (IOException e) {
         LOG.debug("Error finding free port, using default of " + DEFAULT_PORT);
      }
      return DEFAULT_PORT;
   }

   public static <S extends AbstractProtocolServer<?>> S startProtocolServer(int initialPort, Function<Integer, S> serverStarter) {
      S server = null;
      int maxTries = 10;
      int currentTries = 0;
      Throwable lastError = null;
      while (server == null && currentTries < maxTries) {
         try {
            server = serverStarter.apply(initialPort);
         } catch (Throwable t) {
            if (!isBindException(t)) {
               throw t;
            } else {
               LOG.debug("Address already in use: [" + t.getMessage() + "], retrying");
               currentTries++;
               initialPort = findFreePort();
               lastError = t;
            }
         }
      }
      if (server == null && lastError != null)
         throw new AssertionError(lastError);

      return server;
   }


}
