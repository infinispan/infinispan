package org.infinispan.client.hotrod;

import org.infinispan.manager.CacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodServerStarter {

   /**
    * This needs to be different than the one used in the server tests in order to make sure that there's no clash.
    */
   private static final AtomicInteger uniquePort = new AtomicInteger(11312);

   public static HotRodServer startHotRodServer(CacheManager cacheManager) {
      return HotRodTestingUtil.startHotRodServer(cacheManager, uniquePort.incrementAndGet());
   }
}
