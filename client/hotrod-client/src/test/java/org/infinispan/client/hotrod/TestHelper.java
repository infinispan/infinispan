package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.netty.channel.ChannelException;

import java.net.BindException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TestHelper {

   private static final Log log = LogFactory.getLog(TestHelper.class);

   /**
    * This needs to be different than the one used in the server tests in order to make sure that there's no clash.
    */
   private static final AtomicInteger uniquePort = new AtomicInteger(15232);

   public static HotRodServer startHotRodServer(EmbeddedCacheManager cacheManager) {
      // TODO: This is very rudimentary!! HotRodTestingUtil needs a more robust solution where ports are generated randomly and retries if already bound
      HotRodServer server = null;
      int maxTries = 10;
      int currentTries = 0;
      ChannelException lastException = null;
      while (server == null && currentTries < maxTries) {
         try {
            server = HotRodTestingUtil.startHotRodServer(cacheManager, uniquePort.incrementAndGet());
         } catch (ChannelException e) {
            if (!(e.getCause() instanceof BindException)) {
               throw e;
            } else {
               log.debug("Address already in use: [" + e.getMessage() + "], so let's try next port");
               currentTries++;
               lastException = e;
            }
         }
      }
      if (server == null && lastException != null)
         throw lastException;

      return server;
   }

   public static String getServersString(HotRodServer... servers) {
      StringBuilder builder = new StringBuilder();
      for (HotRodServer server : servers) {
         builder.append("localhost").append(':').append(server.getPort()).append(";");
      }
      return builder.toString();
   }

   public static RemoteCacheManager getRemoteCacheManager(HotRodServer server) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer()
         .host(server.getHost())
         .port(server.getPort());
      return new RemoteCacheManager(builder.build());

   }
}
