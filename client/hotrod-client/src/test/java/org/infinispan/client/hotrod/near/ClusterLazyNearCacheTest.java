package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.testng.annotations.Test;

@Deprecated
@Test(groups = "functional", testName = "client.hotrod.near.ClusterLazyNearCacheTest")
public class ClusterLazyNearCacheTest extends ClusterEagerNearCacheTest {

   @Override
   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.LAZY;
   }

   protected void expectNearCacheUpdates(AssertsNearCache<Integer, String> producer,
         Integer key, AssertsNearCache<Integer, String> consumer) {
      producer.get(key, null).expectNearGetNull(key);
      producer.put(key, "v1").expectNearRemove(key, consumer);
      producer.get(key, "v1").expectNearGetNull(key).expectNearPutIfAbsent(key, "v1");
      producer.put(key, "v2").expectNearRemove(key, consumer);
      producer.get(key, "v2").expectNearGetNull(key).expectNearPutIfAbsent(key, "v2");
      producer.remove(key).expectNearRemove(key, consumer);
      producer.get(key, null).expectNearGetNull(key);
   }

}
