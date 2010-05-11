package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "functional", testName = "client.hotrod.HeavyPutTest")
public class HeavyPutTest extends MultipleCacheManagersTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private TcpTransportFactory tcpConnectionFactory;
   private static final String CACHE_NAME = "distributedCache";

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      config.setNumOwners(1);
      CacheManager cm1 = addClusterEnabledCacheManager();
      CacheManager cm2 = addClusterEnabledCacheManager();
      CacheManager cm3 = addClusterEnabledCacheManager();
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      registerCacheManager(cm3);
      cm1.defineConfiguration(CACHE_NAME, config);
      cm2.defineConfiguration(CACHE_NAME, config);
      cm3.defineConfiguration(CACHE_NAME, config);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));

      assert manager(0).getCache(CACHE_NAME) != null;
      assert manager(1).getCache(CACHE_NAME) != null;
      assert manager(2).getCache(CACHE_NAME) != null;

      TestingUtil.blockUntilViewReceived(manager(0).getCache(), 3, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(2).getCache(), ComponentStatus.RUNNING, 10000);

      log.info("Local replication test passed!");

      Properties props = new Properties();
      props.put("hotrod-servers", "localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      remoteCacheManager = new RemoteCacheManager(props);
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
   }

   public void testHeavyPut() {
      List<byte[]> keys = new ArrayList<byte[]>();
      for (int i =0; i < 10000; i++) {
         System.out.println("i = " + i);
         byte[] key = generateKey(i);
         keys.add(key);
         remoteCache.put(new String(key), "value");
      }
      for (byte[] key: keys) {
         assert remoteCache.get(new String(key)).equals("value");
      }
   }

   private byte[] generateKey(int i) {
      Random r = new Random();
      byte[] result = new byte[i];
      r.nextBytes(result);
      return result;
   }
}
