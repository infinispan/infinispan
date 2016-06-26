package org.infinispan.server.core.test;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.AbstractProtocolServer;

/**
 * Infinispan servers testing util
 *
 * @author Galder Zamarreño
 * @author wburns
 */
public class ServerTestingUtil  {

   public static void killServer(AbstractProtocolServer<?> server) {
      try {
         if (server != null) server.stop();
      } catch (Throwable t) {
         LogFactory.getLog(ServerTestingUtil.class).error("Error stopping server", t);
      }
   }

}
