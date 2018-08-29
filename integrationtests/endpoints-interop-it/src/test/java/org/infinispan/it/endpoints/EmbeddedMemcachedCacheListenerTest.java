package org.infinispan.it.endpoints;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import net.spy.memcached.MemcachedClient;

/**
 * Test cache listeners bound to embedded cache and operation over Memcached cache.
 *
 * @author Jiri Holusa [jholusa@redhat.com]
 */
@Test(groups = "functional", testName = "it.endpoints.EmbeddedMemcachedCacheListenerTest")
public class EmbeddedMemcachedCacheListenerTest extends AbstractInfinispanTest {

   EndpointsCacheFactory<String, String> cacheFactory;

   @BeforeMethod
   protected void setup() throws Exception {
      cacheFactory = new EndpointsCacheFactory<String, String>(
            "memcachedCache", new SpyMemcachedMarshaller(), CacheMode.LOCAL, new MemcachedEncoder()).setup();
   }

   @AfterMethod
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testLoadingAndStoringEventsMemcached() throws InterruptedException, ExecutionException, TimeoutException {
      Cache<String, String> embedded = cacheFactory.getEmbeddedCache();
      MemcachedClient remote = cacheFactory.getMemcachedClient();

      TestCacheListener l = new TestCacheListener();
      embedded.addListener(l);

      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      Future<Boolean> future1 = remote.add("k", 0, "v");
      assertTrue(future1.get(60, TimeUnit.SECONDS));

      assertEquals(1, l.createdCounter);
      assertEquals("v", l.created.get("k"));
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      Future<Boolean> future2 = remote.set("key", 0, "value");
      assertTrue(future2.get(60, TimeUnit.SECONDS));

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      Future<Boolean> future3 = remote.set("key", 0, "modifiedValue");
      assertTrue(future3.get(60, TimeUnit.SECONDS));

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(1, l.modifiedCounter);
      assertEquals("modifiedValue", l.modified.get("key"));
      assertTrue(l.visited.isEmpty());

      Future<Boolean> future4 = remote.replace("k", 0, "replacedValue");
      assertTrue(future4.get(60, TimeUnit.SECONDS));

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(2, l.modifiedCounter);
      assertEquals("replacedValue", l.modified.get("k"));
      assertTrue(l.visited.isEmpty());

      //resetting so don't have to type "== 2" etc. all over again
      l.reset();

      Future<Boolean> future5 = remote.delete("key");
      assertTrue(future5.get(60, TimeUnit.SECONDS));

      assertTrue(l.created.isEmpty());
      assertEquals(1, l.removedCounter);
      assertEquals("modifiedValue", l.removed.get("key"));
      assertTrue(l.modified.isEmpty());

      l.reset();

      String value = (String) remote.get("k");
      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertEquals(1, l.visitedCounter);
      assertEquals("replacedValue", l.visited.get("k"));
      assertEquals("replacedValue", value);

      l.reset();
   }

}
