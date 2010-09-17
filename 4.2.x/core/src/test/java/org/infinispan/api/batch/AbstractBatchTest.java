package org.infinispan.api.batch;

import org.infinispan.Cache;
import org.infinispan.test.AbstractInfinispanTest;

import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractBatchTest extends AbstractInfinispanTest {
   protected String getOnDifferentThread(final Cache<String, String> cache, final String key) throws InterruptedException {
      final AtomicReference<String> ref = new AtomicReference<String>();
      Thread t = new Thread() {
         public void run() {
            ref.set(cache.get(key));
         }
      };

      t.start();
      t.join();
      return ref.get();
   }
}
