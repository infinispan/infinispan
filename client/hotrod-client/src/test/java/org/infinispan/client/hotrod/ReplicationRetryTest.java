package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.ReplicationRetryTest", groups = "functional")
public class ReplicationRetryTest extends HitsAwareCacheManagersTest {
   HotRodServer hotRodServer1;
   HotRodServer hotRodServer2;
   HotRodServer hotRodServer3;

   RemoteCache remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private TcpTransportFactory tcpConnectionFactory;
   private Configuration config;
   private RoundRobinBalancingStrategy strategy;

   public ReplicationRetryTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void assertSupportedConfig() {
   }

   @Override
   protected void createCacheManagers() throws Throwable {

      assert cleanup == CleanupPhase.AFTER_METHOD;


      config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      CacheContainer cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheContainer cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheContainer cm3 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      registerCacheManager(cm3);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hrServ2CacheManager.put(getAddress(hotRodServer1), cm1);
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));
      hrServ2CacheManager.put(getAddress(hotRodServer2), cm2);
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));
      hrServ2CacheManager.put(getAddress(hotRodServer3), cm3);

      manager(0).getCache();
      manager(1).getCache();
      manager(2).getCache();

      TestingUtil.blockUntilViewReceived(manager(0).getCache(), 3, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);

      Properties clientConfig = new Properties();
      clientConfig.put("hotrod-servers", "localhost:" + hotRodServer2.getPort());
      clientConfig.put("force-return-value", "true");
      clientConfig.put("maxActive",1); //this ensures that only one server is active at a time

      remoteCacheManager = new RemoteCacheManager(clientConfig);
      remoteCache = remoteCacheManager.getCache();
      tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
      strategy = (RoundRobinBalancingStrategy) tcpConnectionFactory.getBalancer();
      addInterceptors();

      assert super.cacheManagers.size() == 3;

   }


   public void testGet() {
      validateSequenceAndStopServer();
      //now make sure that next call won't fail
      resetStats();
      for (int i = 0; i < 100; i++) {
         assert remoteCache.get("k").equals("v");
      }
   }

   public void testPut() {

      validateSequenceAndStopServer();
      resetStats();

      assert "v".equals(remoteCache.put("k", "v0"));
      for (int i = 1; i < 100; i++) {
         assertEquals("v" + (i-1), remoteCache.put("k", "v"+i));
      }
   }

   public void testRemove() {
      validateSequenceAndStopServer();
      resetStats();

      assertEquals("v", remoteCache.remove("k"));
   }

   public void testContains() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals(true, remoteCache.containsKey("k"));
   }

   public void testGetWithVersion() {
      validateSequenceAndStopServer();
      resetStats();
      VersionedValue value = remoteCache.getVersioned("k");
      assertEquals("v", value.getValue());
   }

   public void testPutIfAbsent() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals(null, remoteCache.putIfAbsent("noSuchKey", "someValue"));
      assertEquals("someValue", remoteCache.get("noSuchKey"));
   }

   public void testReplace() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals("v", remoteCache.replace("k", "v2"));
   }

   public void testReplaceIfUnmodified() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals(false, remoteCache.replaceWithVersion("k", "v2", 12));
   }

   public void testRemoveIfUnmodified() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals(false, remoteCache.removeWithVersion("k", 12));
   }

   public void testClear() {
      validateSequenceAndStopServer();
      resetStats();
      remoteCache.clear();
      assertEquals(false, remoteCache.containsKey("k"));
   }

   private void validateSequenceAndStopServer() {
      resetStats();
      assertNoHits();
      InetSocketAddress expectedServer = strategy.getServers()[strategy.getNextPosition()];
      assertNoHits();
      remoteCache.put("k","v");

      assert strategy.getServers().length == 3;
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = strategy.getServers()[strategy.getNextPosition()];
      remoteCache.put("k2","v2");
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = strategy.getServers()[strategy.getNextPosition()];
      remoteCache.put("k3","v3");
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = strategy.getServers()[strategy.getNextPosition()];
      remoteCache.put("k","v");
      assertOnlyServerHit(expectedServer);

      //this would be the next server to be shutdown
      expectedServer = strategy.getServers()[strategy.getNextPosition()];
      HotRodServer toStop = addr2hrServer.get(expectedServer);
      toStop.stop();
   }
}
