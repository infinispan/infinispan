package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;

@Deprecated
@Test(groups = "functional", testName = "client.hotrod.near.LazyNearCacheTest")
public class LazyNearCacheTest extends EagerNearCacheTest {
   @Override
   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.LAZY;
   }

   @Override
   public void testGetNearCache() {
      assertClient.expectNoNearEvents();
      assertClient.get(1, null).expectNearGetNull(1);
      assertClient.put(1, "v1").expectNearRemove(1);
      assertClient.get(1, "v1").expectNearGetNull(1).expectNearPutIfAbsent(1, "v1");
      assertClient.get(1, "v1").expectNearGetValue(1, "v1");
      assertClient.remove(1).expectNearRemove(1);
      assertClient.get(1, null).expectNearGetNull(1);
   }

   @Override
   public void testGetVersionedNearCache() {
      assertClient.expectNoNearEvents();
      assertClient.getVersioned(1, null).expectNearGetNull(1);
      assertClient.put(1, "v1").expectNearRemove(1);
      assertClient.getVersioned(1, "v1").expectNearGetNull(1).expectNearPutIfAbsent(1, "v1");
      assertClient.getVersioned(1, "v1").expectNearGetValueVersion(1, "v1");
      assertClient.remove(1).expectNearRemove(1);
      assertClient.getVersioned(1, null).expectNearGetNull(1);
   }

   @Override
   public void testUpdateNearCache() {
      assertClient.expectNoNearEvents();
      assertClient.put(1, "v1").expectNearRemove(1);
      assertClient.put(1, "v2").expectNearRemove(1);
      assertClient.get(1, "v2").expectNearGetNull(1).expectNearPutIfAbsent(1, "v2");
      assertClient.get(1, "v2").expectNearGetValue(1, "v2");
      assertClient.put(1, "v3").expectNearRemove(1);
      assertClient.remove(1).expectNearRemove(1);
   }

   @Override
   public void testGetUpdatesNearCache() {
      assertClient.expectNoNearEvents();
      assertClient.put(1, "v1").expectNearRemove(1);

      final AssertsNearCache<Integer, String> newAsserts = createClient();
      withRemoteCacheManager(new RemoteCacheManagerCallable(newAsserts.manager) {
         @Override
         public void call() {
            newAsserts.expectNoNearEvents();
            newAsserts.get(1, "v1").expectNearGetNull(1).expectNearPutIfAbsent(1, "v1");
         }
      });
   }

}
