package org.infinispan.client.hotrod;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TestHelper {

   /**
    * This needs to be different than the one used in the server tests in order to make sure that there's no clash.
    */
   private static final AtomicInteger uniquePort = new AtomicInteger(11312);

   public static HotRodServer startHotRodServer(EmbeddedCacheManager cacheManager) {
      return HotRodTestingUtil.startHotRodServer(cacheManager, uniquePort.incrementAndGet());
   }

   public static String getServersString(HotRodServer... servers) {
      StringBuilder builder = new StringBuilder();
      for (HotRodServer server : servers) {
         builder.append("localhost").append(':').append(server.getPort()).append(";");
      }
      return builder.toString();
   }

   public static Configuration getMultiNodeConfig() {
      Configuration result = new Configuration();
      result.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      result.setSyncReplTimeout(10000);
//      result.setFetchInMemoryState(true);
      result.setSyncCommitPhase(true);
      result.setSyncRollbackPhase(true);
      return result;      
   }
}
