package org.infinispan.it.compatibility;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.AssertJUnit.*;

/**
 * Test cache listeners bound to embedded cache and operation over REST cache.
 *
 * @author Jiri Holusa [jholusa@redhat.com]
 */
@Test(groups = "functional", testName = "it.compatibility.EmbeddedRestCacheListenerTest")
public class EmbeddedRestCacheListenerTest extends AbstractInfinispanTest {

   CompatibilityCacheFactory<String, String> cacheFactory;

   @BeforeMethod
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<String, String>(CacheMode.LOCAL).setup();
   }

   @AfterMethod
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testLoadingAndStoringEventsRest() throws IOException {
      Cache<String, String> embedded = cacheFactory.getEmbeddedCache();
      HttpClient remote = cacheFactory.getRestClient();
      String restUrl = cacheFactory.getRestUrl();

      TestCacheListener l = new TestCacheListener();
      embedded.addListener(l);

      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertTrue(l.visited.isEmpty());

      EntityEnclosingMethod put = new PutMethod(restUrl + "/k");
      put.setRequestEntity(new ByteArrayRequestEntity(
            "v".getBytes(), "application/octet-stream"));
      remote.executeMethod(put);

      assertEquals(1, l.createdCounter);
      assertEquals("v".getBytes(), (byte[]) l.created.get("k"));
      assertTrue(l.removed.isEmpty());
      assertEquals(0, l.modifiedCounter);
      assertTrue(l.visited.isEmpty());


      EntityEnclosingMethod put2 = new PutMethod(restUrl + "/key");
      put2.setRequestEntity(new ByteArrayRequestEntity(
            "value".getBytes(), "application/octet-stream"));
      remote.executeMethod(put2);

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(0, l.modifiedCounter);
      assertTrue(l.visited.isEmpty());

      EntityEnclosingMethod put3 = new PutMethod(restUrl + "/key");
      put3.setRequestEntity(new ByteArrayRequestEntity(
            "modifiedValue".getBytes(), "application/octet-stream"));
      remote.executeMethod(put3);

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(1, l.modifiedCounter);
      assertEquals("modifiedValue".getBytes(), (byte[]) l.modified.get("key"));
      assertTrue(l.visited.isEmpty());

      EntityEnclosingMethod post = new PutMethod(restUrl + "/k");
      post.setRequestEntity(new ByteArrayRequestEntity(
            "replacedValue".getBytes(), "application/octet-stream"));
      remote.executeMethod(post);

      assertEquals(2, l.createdCounter);
      assertTrue(l.removed.isEmpty());
      assertEquals(2, l.modifiedCounter);
      assertEquals("replacedValue".getBytes(), (byte[]) l.modified.get("k"));
      assertTrue(l.visited.isEmpty());

      //resetting so don't have to type "== 2" etc. all over again
      l.reset();

      DeleteMethod delete = new DeleteMethod(restUrl + "/key");
      remote.executeMethod(delete);

      assertTrue(l.created.isEmpty());
      assertEquals(1, l.removedCounter);
      assertEquals("modifiedValue".getBytes(), (byte[]) l.removed.get("key"));
      assertTrue(l.modified.isEmpty());

      l.reset();

      GetMethod get = new GetMethod(restUrl + "/k");
      remote.executeMethod(get);

      assertTrue(l.created.isEmpty());
      assertTrue(l.removed.isEmpty());
      assertTrue(l.modified.isEmpty());
      assertEquals(1, l.visitedCounter);
      assertEquals("replacedValue".getBytes(), (byte[]) l.visited.get("k"));

      l.reset();
   }

}
