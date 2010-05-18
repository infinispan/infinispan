package org.infinispan.client.hotrod;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.CSAIntegrationTest")
public class CSAIntegrationTest extends MultipleCacheManagersTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private TcpTransportFactory tcpConnectionFactory;
   private static final String CACHE_NAME = "distributedCache";

   private static Log log = LogFactory.getLog(CSAIntegrationTest.class);

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      config.setUnsafeUnreliableReturnValues(true);
      CacheManager cm1 = addClusterEnabledCacheManager();
      CacheManager cm2 = addClusterEnabledCacheManager();
      CacheManager cm3 = addClusterEnabledCacheManager();
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

      manager(0).getCache(CACHE_NAME).put("k", "v");
      manager(0).getCache(CACHE_NAME).get("k").equals("v");
      manager(1).getCache(CACHE_NAME).get("k").equals("v");
      manager(2).getCache(CACHE_NAME).get("k").equals("v");

      log.info("Local replication test passed!");

      //Important: this only connects to one of the two servers!
      Properties props = new Properties();
      props.put("hotrod-servers", "localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
//      props.put("marshaller", ByteMarshaller.class.getName());
      remoteCacheManager = new RemoteCacheManager(props);
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);

      tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
   }

   public void testHashInfoRetrieved() throws InterruptedException {
      assert tcpConnectionFactory.getServers().size() == 1;
      for (int i = 0; i < 10; i++) {
         remoteCache.put("k", "v");
         if (tcpConnectionFactory.getServers().size() == 3) break;
         Thread.sleep(1000);
      }
      assertEquals(3, tcpConnectionFactory.getServers().size());
      assertNotNull(tcpConnectionFactory.getConsistentHash());
   }

   @Test(dependsOnMethods = "testHashInfoRetrieved")
   public void testCorrectSetup() {
      remoteCache.put("k", "v");
      assert remoteCache.get("k").equals("v");
   }


   @Test(dependsOnMethods = "testCorrectSetup")
   public void testHashFunctionReturnsSameValues() {
      Map<InetSocketAddress, CacheManager> add2Cm = new HashMap<InetSocketAddress, CacheManager>();
      add2Cm.put(new InetSocketAddress(hotRodServer1.getHost(), hotRodServer1.getPort()), manager(0));
      add2Cm.put(new InetSocketAddress(hotRodServer2.getHost(), hotRodServer2.getPort()), manager(1));
      add2Cm.put(new InetSocketAddress(hotRodServer3.getHost(), hotRodServer3.getPort()), manager(2));

      for (int i = 0; i < 1000; i++) {
         byte[] key = generateKey(i);
         TcpTransport transport = (TcpTransport) tcpConnectionFactory.getTransport(key);
         InetSocketAddress serverAddress = transport.getServerAddress();
         CacheManager cacheManager = add2Cm.get(serverAddress);
         assertNotNull("For server address " + serverAddress + " found " + cacheManager + ". Map is: " + add2Cm, cacheManager);
         DistributionManager distributionManager = cacheManager.getCache(CACHE_NAME).getAdvancedCache().getDistributionManager();
         assert distributionManager.isLocal(key);
         tcpConnectionFactory.releaseTransport(transport);
      }
   }

   @Test(dependsOnMethods = "testHashFunctionReturnsSameValues")
   public void testRequestsGoToExpectedServer() {

      addCacheMgmtInterceptor(manager(0).getCache(CACHE_NAME));
      addCacheMgmtInterceptor(manager(1).getCache(CACHE_NAME));
      addCacheMgmtInterceptor(manager(2).getCache(CACHE_NAME));

      List<byte[]> keys = new ArrayList<byte[]>();
      for (int i = 0; i < 500; i++) {
         byte[] key = generateKey(i);
         keys.add(key);
         remoteCache.put(new String(key), "value");
      }

      assertMisses(false);

      log.info("Right before first get.");

      for (byte[] key : keys) {
         assert remoteCache.get(new String(key)).equals("value");
         assertMisses(false);
      }
      assertMisses(false);

      remoteCache.get("noSuchKey");
      assertMisses(true);
   }

   private void addCacheMgmtInterceptor(Cache<Object, Object> cache) {
      CacheMgmtInterceptor interceptor = new CacheMgmtInterceptor();
      cache.getAdvancedCache().addInterceptor(interceptor, 1);
   }

   private void assertMisses(boolean expected) {
      int misses = getMissCount(manager(0).getCache(CACHE_NAME));
      misses += getMissCount(manager(1).getCache(CACHE_NAME));
      misses += getMissCount(manager(2).getCache(CACHE_NAME));

      if (expected) {
         assert misses > 0;
      } else {
         assertEquals(0, misses);
      }
   }

   private int getMissCount(Cache<Object, Object> cache) {
      List<CommandInterceptor> interceptorChain = cache.getAdvancedCache().getInterceptorChain();
      for (CommandInterceptor interceptor : interceptorChain) {
         if (interceptor instanceof CacheMgmtInterceptor) {
            CacheMgmtInterceptor cacheMgmtInterceptor = (CacheMgmtInterceptor) interceptor;
            return (int) cacheMgmtInterceptor.getMisses();
         }
      }
      throw new IllegalStateException();
   }

   private byte[] generateKey(int i) {
      Random r = new Random();
      byte[] result = new byte[i];
      r.nextBytes(result);
      return result;
   }
}
