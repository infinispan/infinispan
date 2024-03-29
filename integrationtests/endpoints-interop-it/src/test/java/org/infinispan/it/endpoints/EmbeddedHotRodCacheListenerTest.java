package org.infinispan.it.endpoints;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test cache listeners bound to embedded cache and operation over HotRod cache.
 *
 * @author Jiri Holusa [jholusa@redhat.com]
 */
@Test(groups = "functional", testName = "it.endpoints.EmbeddedHotRodCacheListenerTest")
public class EmbeddedHotRodCacheListenerTest extends AbstractInfinispanTest {

   EndpointsCacheFactory<String, String> cacheFactory;

   @BeforeMethod
   protected void setup() throws Exception {
      cacheFactory = new EndpointsCacheFactory.Builder<String, String>().withCacheMode(CacheMode.LOCAL).build();
   }

   @AfterMethod
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testLoadingAndStoringEventsHotRod1() {
      Cache<String, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<String, String> remote = cacheFactory.getHotRodCache();

      TestCacheListener l = new TestCacheListener();
      embedded.addListener(l);

      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      remote.put("k", "v");
      assertEquals(1, l.createdCounter);
      assertEquals("v", l.created.get("k"));
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      remote.put("key", "value");
      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      remote.put("key", "modifiedValue");
      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(1, l.modifiedCounter);
      assertEquals("modifiedValue", l.modified.get("key"));
      assertTrue(l.visited.isEmpty());

      remote.replace("k", "replacedValue");
      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(2, l.modifiedCounter);
      assertEquals("replacedValue", l.modified.get("k"));
      assertTrue(l.visited.isEmpty());

      //resetting so don't have to type "== 2" etc. all over again
      l.reset();

      Set<String> keys = new HashSet<>();
      keys.add("k");
      keys.add("key");
      Map<String, String> results = remote.getAll(keys);
      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertEquals(2, l.visitedCounter);
      assertEquals("replacedValue", l.visited.get("k"));
      assertEquals("modifiedValue", l.visited.get("key"));

      l.reset();

      remote.remove("key");
      assertTrue(l.created.isEmpty());
      assertEquals(1, l.removedCounter);
      assertEquals("modifiedValue", l.removed.get("key"));
      assertTrue(l.modified.isEmpty());

      l.reset();

      String value = remote.get("k");
      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertEquals(1, l.visitedCounter);
      assertEquals("replacedValue", l.visited.get("k"));

      l.reset();
   }

   public void testLoadingAndStoringEventsHotRod2() {
      Cache<String, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<String, String> remote = cacheFactory.getHotRodCache();

      TestCacheListener l = new TestCacheListener();
      embedded.addListener(l);

      Map<String, String> tmp = new HashMap<String, String>();
      tmp.put("key", "value");
      tmp.put("key2", "value2");

      remote.putAll(tmp);
      assertEquals(2, l.createdCounter); //one event for every put
      assertEquals("value", l.created.get("key"));
      assertEquals("value2", l.created.get("key2"));
      assertTrue(l.modified.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.visited.isEmpty());

      l.reset();

      remote.putIfAbsent("newKey", "newValue");
      assertEquals(1, l.createdCounter);
      assertEquals("newValue", l.created.get("newKey"));
      assertTrue(l.modified.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.visited.isEmpty());

      l.reset();

      remote.putIfAbsent("newKey", "shouldNotBeAdded");
      assertTrue(l.created.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.removed.isEmpty());

      l.reset();
   }

   public void testLoadingAndStoringWithVersionEventsHotRod() {
      Cache<String, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<String, String> remote = cacheFactory.getHotRodCache();

      TestCacheListener l = new TestCacheListener();
      embedded.addListener(l);

      remote.put("key", "value");
      VersionedValue oldVersionedValue = remote.getWithMetadata("key");
      assertEquals("value", oldVersionedValue.getValue());
      assertEquals(1, l.createdCounter);
      assertTrue(l.modified.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertEquals(1, l.visitedCounter);
      assertEquals("value", l.visited.get("key"));

      remote.put("key2", "value2");
      remote.put("key", "outOfVersionValue");
      VersionedValue newVersionedValue = remote.getWithMetadata("key2");

      l.reset();

      remote.removeWithVersion("key", oldVersionedValue.getVersion());
      assertTrue(l.created.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertEquals(1, l.visitedCounter);

      l.reset();

      remote.removeWithVersion("key2", newVersionedValue.getVersion());
      assertTrue(l.created.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertEquals(1, l.removedCounter);
      assertEquals("value2", l.removed.get("key2"));
      assertEquals(1, l.visitedCounter);

      remote.put("newKey", "willBeOutOfDate");
      VersionedValue oldVersionedValueToBeReplaced = remote.getWithMetadata("newKey");
      remote.put("newKey", "changedValue");

      l.reset();

      remote.replaceWithVersion("newKey", "tryingToChangeButShouldNotSucceed", oldVersionedValueToBeReplaced.getVersion());
      assertTrue(l.created.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.visited.isEmpty());

      remote.put("newKey2", "willBeSuccessfullyChanged");
      VersionedValue newVersionedValueToBeReplaced = remote.getWithMetadata("newKey2");

      l.reset();

      remote.replaceWithVersion("newKey2", "successfulChange", newVersionedValueToBeReplaced.getVersion());
      assertTrue(l.created.isEmpty());
      assertEquals(1, l.modifiedCounter);
      assertEquals("successfulChange", l.modified.get("newKey2"));
      assertTrue(l.removed.isEmpty());
      assertTrue(l.visited.isEmpty());

      l.reset();
   }

   public void testLoadingAndStoringAsyncEventsHotRod() throws InterruptedException, ExecutionException, TimeoutException {
      Cache<String, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<String, String> remote = cacheFactory.getHotRodCache();

      TestCacheListener l = new TestCacheListener();
      embedded.addListener(l);

      CompletableFuture future = remote.putAsync("k", "v");
      future.get(60, TimeUnit.SECONDS);
      assertEquals(1, l.createdCounter);
      assertEquals("v", l.created.get("k"));
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      CompletableFuture future2 = remote.putAsync("key", "value");
      future2.get(60, TimeUnit.SECONDS);
      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      CompletableFuture future3 = remote.putAsync("key", "modifiedValue");
      future3.get(60, TimeUnit.SECONDS);
      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(1, l.modifiedCounter);
      assertEquals("modifiedValue", l.modified.get("key"));
      assertTrue(l.visited.isEmpty());

      CompletableFuture future4 = remote.replaceAsync("k", "replacedValue");
      future4.get(60, TimeUnit.SECONDS);
      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(2, l.modifiedCounter);
      assertEquals("replacedValue", l.modified.get("k"));
      assertTrue(l.visited.isEmpty());

      //resetting so don't have to type "== 2" etc. all over again
      l.reset();

      CompletableFuture future5 = remote.removeAsync("key");
      future5.get(60, TimeUnit.SECONDS);
      assertTrue(l.created.isEmpty());
      assertEquals(1, l.removedCounter);
      assertEquals("modifiedValue", l.removed.get("key"));
      assertTrue(l.modified.isEmpty());

      l.reset();

      CompletableFuture future6 = remote.getAsync("k");
      future6.get(60, TimeUnit.SECONDS);
      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertEquals(1, l.visitedCounter);
      assertEquals("replacedValue", l.visited.get("k"));

      l.reset();
   }

}
