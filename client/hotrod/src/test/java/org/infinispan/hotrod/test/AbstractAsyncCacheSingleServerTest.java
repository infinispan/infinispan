package org.infinispan.hotrod.test;

import static org.infinispan.hotrod.AwaitAssertions.await;

import org.infinispan.api.Infinispan;
import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.async.AsyncContainer;

/**
 * @since 14.0
 */
public abstract class AbstractAsyncCacheSingleServerTest<K, V> extends AbstractSingleHotRodServerTest<AsyncCache<K, V>> {

   public void teardown() {
      assert container instanceof AsyncContainer : "Could not destroy AsyncCache";
      await(((AsyncContainer) container).caches().remove(cacheName));
   }

   @Override
   Infinispan container() {
      if (container != null)
         return container;
      return server.getClient().async();
   }

   @Override
   AsyncCache<K, V> cache() {
      if (cache != null) {
         return cache;
      }

      assert container instanceof AsyncContainer : "Could not create AsyncCache";
      return await(((AsyncContainer) container).caches().create(cacheName, "template"));
   }
}
