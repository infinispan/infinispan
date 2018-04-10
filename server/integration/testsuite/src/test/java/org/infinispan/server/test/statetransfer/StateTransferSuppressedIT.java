package org.infinispan.server.test.statetransfer;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests that entries are fetch correct from cache where initial state transfer is is async of completely turned off.
 *
 * @author vjuranek
 * @since 9.3
 */
@RunWith(Arquillian.class)
public class StateTransferSuppressedIT {

   private static final Logger log = Logger.getLogger(RebalanceSuppressIT.class);

   protected static final String CONTAINER1 = "suppress-state-transfer-1";
   protected static final String CONTAINER2 = "suppress-state-transfer-2";

   protected static final int NUMBER_ENTRIES = 100;

   @InfinispanResource(CONTAINER1)
   protected RemoteInfinispanServer server1;
   @InfinispanResource(CONTAINER2)
   protected RemoteInfinispanServer server2;

   private static final String CACHE_NAME_DIST_ST_ASYNC = "dist-state-transfer-async";
   private static final String CACHE_NAME_DIST_ST_OFF = "dist-state-transfer-off";
   private static final String CACHE_NAME_REPL_ST_ASYNC = "repl-state-transfer-async";
   private static final String CACHE_NAME_REPL_ST_OFF = "repl-state-transfer-off";

   @ArquillianResource
   protected ContainerController controller;

   private RemoteCacheManager rcm1;
   private RemoteCacheManager rcm2;
   private RemoteCache cache1;
   private RemoteCache cache2;

   @Before
   public void setUp() throws Exception {
      rcm1 = ITestUtils.createCacheManager(server1);
   }

   @After
   public void tearDown() throws Exception {
      if (null != rcm1)
         rcm1.stop();
      if (null != rcm2)
         rcm2.stop();
   }

   @Test
   @WithRunningServer(@RunningServer(name = CONTAINER1))
   public void testDistAsync() {
      testStateTransfert(CACHE_NAME_DIST_ST_ASYNC);
   }

   @Ignore //https://issues.jboss.org/browse/ISPN-8908
   @Test
   @WithRunningServer(@RunningServer(name = CONTAINER1))
   public void testDistOff() {
      testStateTransfert(CACHE_NAME_DIST_ST_OFF);
   }

   @Test
   @WithRunningServer(@RunningServer(name = CONTAINER1))
   public void testReplAsync() {
      testStateTransfert(CACHE_NAME_REPL_ST_ASYNC);
   }

   @Ignore //https://issues.jboss.org/browse/ISPN-8908
   @Test
   @WithRunningServer(@RunningServer(name = CONTAINER1))
   public void testReplOff() {
      testStateTransfert(CACHE_NAME_REPL_ST_OFF);
   }

   protected void testStateTransfert(String cacheName) {
      cache1 = rcm1.getCache(cacheName);
      populateCache(cache1, NUMBER_ENTRIES);
      controller.start(CONTAINER2);
      rcm2 = ITestUtils.createCacheManager(server2);
      cache2 = rcm2.getCache(cacheName);
      assertCache(cache2, NUMBER_ENTRIES);
      controller.stop(CONTAINER2);
   }

   private void populateCache(RemoteCache cache, int count) {
      for (int i = 0; i < count; i++) {
         cache.put("key" + i, "value" + i);
      }
      assertCache(cache, count);
   }

   protected void assertCache(RemoteCache cache, int count) {
      assertEquals(count, cache.size());
      for (int i = 0; i < count; i++) {
         assertEquals("value" + i, cache.get("key" + i));
      }
   }

}
