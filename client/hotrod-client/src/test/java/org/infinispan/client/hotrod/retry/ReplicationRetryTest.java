package org.infinispan.client.hotrod.retry;

import static org.testng.Assert.assertEquals;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.retry.ReplicationRetryTest", groups = "functional")
public class ReplicationRetryTest extends AbstractRetryTest {

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

      assertEquals(remoteCache.put("k", "v0"), "v");
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

   public void testBulkGet() {
      validateSequenceAndStopServer();
      resetStats();
      Map map = remoteCache.getBulk();
      assertEquals(3, map.size());
   }

   private void validateSequenceAndStopServer() {
      ConsistentHash consistentHash = tcpTransportFactory.getConsistentHash(RemoteCacheManager.cacheNameBytes());
      SocketAddress expectedServer;

      resetStats();
      assertNoHits();
      expectedServer = consistentHash.getServer(HotRodTestingUtil.marshall("k"));
      assertNoHits();
      remoteCache.put("k","v");

      assert strategy.getServers().length == 3;
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = consistentHash.getServer(HotRodTestingUtil.marshall("k2"));
      remoteCache.put("k2","v2");
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = consistentHash.getServer(HotRodTestingUtil.marshall("k3"));
      remoteCache.put("k3","v3");
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = consistentHash.getServer(HotRodTestingUtil.marshall("k"));
      assertEquals("v", remoteCache.put("k","v"));
      assertOnlyServerHit(expectedServer);

      //this would be the next server to be shutdown
      expectedServer = consistentHash.getServer(HotRodTestingUtil.marshall("k"));
      HotRodServer toStop = addr2hrServer.get(expectedServer);
      toStop.stop();
      for (Iterator<EmbeddedCacheManager> ecmIt = cacheManagers.iterator(); ecmIt.hasNext();) {
         if (ecmIt.next().getAddress().equals(expectedServer)) ecmIt.remove();
      }
      TestingUtil.waitForStableTopology(caches());
   }

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }
}
