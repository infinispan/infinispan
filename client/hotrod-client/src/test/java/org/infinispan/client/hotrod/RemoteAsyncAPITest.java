package org.infinispan.client.hotrod;

import org.infinispan.commons.util.concurrent.FutureListener;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertNotEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.RemoteAsyncAPITest")
public class RemoteAsyncAPITest extends SingleCacheManagerTest {
   private HotRodServer hotrodServer;
   private RemoteCacheManager rcm;
   private RemoteCache<String, String> c;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration());
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      Properties props = new Properties();
      props.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      props.put("infinispan.client.hotrod.force_return_values","true");
      props.put("testOnBorrow", "false");
      rcm = new RemoteCacheManager(props);
      c = rcm.getCache(true);
   }

   @AfterClass
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      killRemoteCacheManager(rcm);
      killServers(hotrodServer);
   }

   public void testPutAsync() throws Exception {
      // put
      Future<String> f = c.putAsync("k", "v");
      testFuture(f, null);
      testK("v");

      f = c.putAsync("k", "v2");
      testFuture(f, "v");
      testK("v2");
   }

   public void testPutAsyncWithListener() throws Exception {
      NotifyingFuture<String> f = c.putAsync("k", "v");
      testFutureWithListener(f, null);
      testK("v");

      f = c.putAsync("k", "v2");
      testFutureWithListener(f, "v");
      testK("v2");
   }

   public void testPutAllAsync() throws Exception {
      Future<Void> f = c.putAllAsync(Collections.singletonMap("k", "v3"));
      testFuture(f, null);
      testK("v3");
   }

   public void testPutAllAsyncWithListener() throws Exception {
      NotifyingFuture<Void> f = c.putAllAsync(Collections.singletonMap("k", "v3"));
      testFutureWithListener(f, null);
      testK("v3");
   }

   public void testPutIfAbsentAsync() throws Exception {
      c.put("k", "v3");
      testK("v3");

      Future<String> f = c.putIfAbsentAsync("k", "v4");
      testFuture(f, "v3");
      assertEquals("v3", c.remove("k"));

      f = c.putIfAbsentAsync("k", "v5");
      testFuture(f, null);
      testK("v5");
   }

   public void testPutIfAbsentAsyncWithListener() throws Exception {
      c.put("k", "v3");
      testK("v3");

      NotifyingFuture<String> f = c.putIfAbsentAsync("k", "v4");
      testFutureWithListener(f, "v3");
      assertEquals("v3", c.remove("k"));

      f = c.putIfAbsentAsync("k", "v5");
      testFutureWithListener(f, null);
      testK("v5");
   }

   public void testRemoveAsync() throws Exception {
      c.put("k","v3");
      testK("v3");

      Future<String> f = c.removeAsync("k");
      testFuture(f, "v3");
      testK(null);
   }

   public void testRemoveAsyncWithListener() throws Exception {
      c.put("k","v3");
      testK("v3");

      NotifyingFuture<String> f = c.removeAsync("k");
      testFutureWithListener(f, "v3");
      testK(null);
   }

   public void testGetAsync() throws Exception {
      c.put("k", "v");
      testK("v");

      Future<String> f = c.getAsync("k");
      testFuture(f, "v");
      testK("v");
   }

   public void testGetAsyncWithListener() throws Exception {
      c.put("k", "v");
      testK("v");

      NotifyingFuture<String> f = c.getAsync("k");
      testFutureWithListener(f, "v");
   }

   public void testRemoveWithVersionAsync() throws Exception {
      c.put("k","v4");
      VersionedValue value = c.getVersioned("k");

      Future<Boolean> f = c.removeWithVersionAsync("k", value.getVersion() + 1);
      testFuture(f, false);
      testK("v4");

      f = c.removeWithVersionAsync("k", value.getVersion());
      testFuture(f, true);
      testK(null);
   }

   public void testRemoveWithVersionAsyncWithListener() throws Exception {
      c.put("k","v4");
      VersionedValue value = c.getVersioned("k");

      NotifyingFuture<Boolean> f = c.removeWithVersionAsync("k", value.getVersion() + 1);
      testFutureWithListener(f, false);
      testK("v4");

      f = c.removeWithVersionAsync("k", value.getVersion());
      testFutureWithListener(f, true);
      testK(null);
   }

   public void testReplaceAsync() throws Exception {
      testK(null);
      Future<String> f = c.replaceAsync("k", "v5");
      testFuture(f, null);
      testK(null);

      c.put("k", "v");
      testK("v");
      f = c.replaceAsync("k", "v5");
      testFuture(f, "v");
      testK("v5");
   }

   public void testReplaceAsyncWithListener() throws Exception {
      testK(null);
      NotifyingFuture<String> f = c.replaceAsync("k", "v5");
      testFutureWithListener(f, null);
      testK(null);

      c.put("k", "v");
      testK("v");
      f = c.replaceAsync("k", "v5");
      testFutureWithListener(f, "v");
      testK("v5");
   }

   public void testReplaceWithVersionAsync() throws Exception {
      c.put("k", "v");
      VersionedValue versioned1 = c.getVersioned("k");

      Future<Boolean> f = c.replaceWithVersionAsync("k", "v2", versioned1.getVersion());
      testFuture(f, true);

      VersionedValue versioned2 = c.getVersioned("k");
      assertNotEquals(versioned1.getVersion(), versioned2.getVersion());
      assertEquals(versioned2.getValue(), "v2");

      f = c.replaceWithVersionAsync("k", "v3", versioned1.getVersion());
      testFuture(f, false);
      testK("v2");
   }

   public void testReplaceWithVersionAsyncWithListener() throws Exception {
      c.put("k", "v");
      VersionedValue versioned1 = c.getVersioned("k");

      NotifyingFuture<Boolean> f = c.replaceWithVersionAsync("k", "v2", versioned1.getVersion());
      testFutureWithListener(f, true);

      VersionedValue versioned2 = c.getVersioned("k");
      assertNotEquals(versioned1.getVersion(), versioned2.getVersion());
      assertEquals(versioned2.getValue(), "v2");

      f = c.replaceWithVersionAsync("k", "v3", versioned1.getVersion());
      testFutureWithListener(f, false);
      testK("v2");
   }

   private <T> void testK(T expected) {
      assertEquals(expected, c.get("k"));
   }

   private <T> void testFuture(Future<T> f, T expected) throws ExecutionException, InterruptedException {
      assertNotNull(f);
      assertFalse(f.isCancelled());
      T value = f.get();
      assertEquals("Obtained " + value, expected, value);
      assertTrue(f.isDone());
   }

   private <T> void testFutureWithListener(NotifyingFuture<T> f, T expected) throws InterruptedException {
      assertNotNull(f);
      AtomicReference<Throwable> ex = new AtomicReference<Throwable>();
      CountDownLatch latch = new CountDownLatch(1);
      f.attachListener(new TestingListener<T>(expected, ex, latch));
      if (!latch.await(5, TimeUnit.SECONDS)) {
         fail("Not finished within 5 seconds");
      }
      if (ex.get() != null) {
         throw new AssertionError(ex.get());
      }
   }

   private static class TestingListener<T> implements FutureListener<T> {
      private final T expected;
      private final AtomicReference<Throwable> exception;
      private final CountDownLatch latch;

      private TestingListener(T expected, AtomicReference<Throwable> exception, CountDownLatch latch) {
         this.expected = expected;
         this.exception = exception;
         this.latch = latch;
      }

      @Override
      public void futureDone(Future<T> future) {
         try {
            assertNotNull(future);
            assertFalse(future.isCancelled());
            assertTrue(future.isDone());
            T value = future.get();
            assertEquals("Obtained " + value, expected, value);
         } catch (Throwable t) {
            exception.set(t);
         } finally {
            latch.countDown();
         }
      }
   }
}
