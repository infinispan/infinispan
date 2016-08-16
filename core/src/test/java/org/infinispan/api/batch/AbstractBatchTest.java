package org.infinispan.api.batch;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.Cache;
import org.infinispan.test.AbstractInfinispanTest;

public abstract class AbstractBatchTest extends AbstractInfinispanTest {
   protected String getOnDifferentThread(final Cache<String, String> cache, final String key) throws InterruptedException {
      final AtomicReference<String> ref = new AtomicReference<String>();
      Thread t = new Thread() {
         public void run() {
            cache.startBatch();
            ref.set(cache.get(key));
            cache.endBatch(true);
         }
      };

      t.start();
      t.join();
      return ref.get();
   }
}
