package org.infinispan.it.endpoints;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test cache listeners bound to embedded cache and operation over REST cache.
 *
 * @author Jiri Holusa [jholusa@redhat.com]
 */
@Test(groups = "functional", testName = "it.endpoints.EmbeddedRestCacheListenerTest")
public class EmbeddedRestCacheListenerTest extends AbstractInfinispanTest {

   EndpointsCacheFactory<String, String> cacheFactory;

   @BeforeMethod
   protected void setup() throws Exception {
      cacheFactory = new EndpointsCacheFactory.Builder<String, String>().withCacheMode(CacheMode.LOCAL).build();
   }

   @AfterMethod
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testLoadingAndStoringEventsRest() {
      Cache<String, String> embedded = cacheFactory.getEmbeddedCache();
      RestCacheClient remote = cacheFactory.getRestCacheClient();

      TestCacheListener l = new TestCacheListener();
      embedded.addListener(l);

      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      RestEntity v = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "v".getBytes());
      join(remote.put("k", v));

      assertEquals(1, l.createdCounter);
      assertEquals("v".getBytes(), (byte[]) l.created.get("k"));
      assertTrue(l.removed.isEmpty());
      assertEquals(0, l.modifiedCounter);
      assertTrue(l.visited.isEmpty());


      RestEntity value = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "value".getBytes());
      join(remote.put("key", value));

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(0, l.modifiedCounter);
      assertTrue(l.visited.isEmpty());

      RestEntity modifiedValue = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "modifiedValue".getBytes());
      join(remote.put("key", modifiedValue));

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(1, l.modifiedCounter);
      assertEquals("modifiedValue".getBytes(), (byte[]) l.modified.get("key"));
      assertTrue(l.visited.isEmpty());

      RestEntity replacedValue = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "replacedValue".getBytes());
      join(remote.put("k", replacedValue));

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(2, l.modifiedCounter);
      assertEquals("replacedValue".getBytes(), (byte[]) l.modified.get("k"));
      assertTrue(l.visited.isEmpty());

      //resetting so don't have to type "== 2" etc. all over again
      l.reset();

      join(remote.remove("key"));

      assertTrue(l.created.isEmpty());
      assertEquals(1, l.removedCounter);
      assertEquals("modifiedValue".getBytes(), (byte[]) l.removed.get("key"));
      assertTrue(l.modified.isEmpty());

      l.reset();

      join(remote.get("k"));

      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertEquals(1, l.visitedCounter);
      assertEquals("replacedValue".getBytes(), (byte[]) l.visited.get("k"));

      l.reset();
   }

}
