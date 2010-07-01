package org.infinispan.client.hotrod.retry;

import org.infinispan.client.hotrod.HitsAwareCacheManagersTest;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.VersionedValue;
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

   @Override
   protected Configuration getCacheConfig() {
      return getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
   }
}
