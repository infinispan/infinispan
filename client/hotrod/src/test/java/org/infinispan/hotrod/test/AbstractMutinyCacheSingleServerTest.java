package org.infinispan.hotrod.test;

import static org.infinispan.hotrod.AwaitAssertions.await;

import org.infinispan.api.Infinispan;
import org.infinispan.api.mutiny.MutinyCache;
import org.infinispan.api.mutiny.MutinyContainer;

/**
 * @since 14.0
 * @param <K>: Cache key type.
 * @param <V>: Cache value type.
 */
public abstract class AbstractMutinyCacheSingleServerTest<K, V> extends AbstractSingleHotRodServerTest<MutinyCache<K, V>> {

   @Override
   protected void teardown() {
      assert container instanceof MutinyContainer : "Could not destroy MutinyCache";
      await(((MutinyContainer) container).caches().remove(cacheName).convert().toCompletionStage());
   }

   @Override
   Infinispan container() {
      if (container != null)
         return container;
      return server.getClient().mutiny();
   }

   @Override
   MutinyCache<K, V> cache() {
      if (cache != null) {
         return cache;
      }

      assert container instanceof MutinyContainer : "Could not create MutinyCache";
      return await(((MutinyContainer) container).caches().create(cacheName, "template"));
   }
}
