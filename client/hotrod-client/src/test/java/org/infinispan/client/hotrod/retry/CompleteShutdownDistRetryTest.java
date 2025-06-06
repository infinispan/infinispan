package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.HitsAwareCacheManagersTest;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.retry.CompleteShutdownDistRetryTest")
public class CompleteShutdownDistRetryTest extends HitsAwareCacheManagersTest {

   List<SocketAddress> addrs;
   List<byte[]> keys;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getConfiguration();
      createHotRodServers(3, builder);
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationBuilder() {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.serialization().addContextInitializer(new SCIImpl());
      return gcb;
   }

   @Override
   protected void assertOnlyServerHit(SocketAddress serverAddress) {
      super.assertOnlyServerHit(serverAddress);
      resetStats(); // reset stats after checking that only server got hit
   }

   public void testRetryAfterCompleteShutdown() {
      RemoteCache<byte[], String> client = client(0).getCache();
      int initialServerPort = addr2hrServer.values().iterator().next().getPort();
      log.infof("InitialServerPort = %d", initialServerPort);

      addrs = getSocketAddressList();
      keys = genKeys();

      assertNoHits();

      assertPutAndGet(client, 0, "zero");
      assertPutAndGet(client, 1, "one");
      assertPutAndGet(client, 2, "two");

      killServer();
      killServer();
      assertNull(client.get(keys.get(0))); // data gone
      assertNull(client.get(keys.get(1))); // data gone

      resetStats();
      assertEquals("two", client.get(keys.get(2)));
      assertOnlyServerHit(addrs.get(2));

      killServer();
      try {
         assertEquals("two", client.get(keys.get(2)));
         fail("Should have thrown exception");
      } catch (TransportException e) {
         // Ignore, expected
      }

      resetStats();

      addHotRodServer(getConfiguration(), initialServerPort);
      addHotRodServer(getConfiguration());
      addHotRodServer(getConfiguration());
      addInterceptors();

      keys = genKeys();
      addrs = getSocketAddressList();

      assertNoHits();
      assertPutAndGet(client, 0, "zero");
      assertPutAndGet(client, 1, "one");
      assertPutAndGet(client, 2, "two");
   }

   private void assertPutAndGet(RemoteCache<byte[], String> client, int nodeIndex, String value) {
      client.put(keys.get(nodeIndex), value);
      assertOnlyServerHit(addrs.get(nodeIndex));
      assertEquals(value, client.get(keys.get(nodeIndex)));
      assertOnlyServerHit(addrs.get(nodeIndex));
   }

   private List<byte[]> genKeys() {
      List<byte[]> keys = new ArrayList<>();
      for (Map.Entry<SocketAddress, HotRodServer> entry : addr2hrServer.entrySet()) {
         keys.add(HotRodClientTestingUtil.getKeyForServer(entry.getValue()));
      }
      return keys;
   }

   private List<SocketAddress> getSocketAddressList() {
      return new ArrayList<>(addr2hrServer.keySet());
   }

   private ConfigurationBuilder getConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numSegments(3).numOwners(1);
      builder.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(new StableControlledConsistentHashFactory());
      return hotRodCacheConfiguration(builder);
   }
}
