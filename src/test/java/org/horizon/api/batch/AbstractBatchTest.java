package org.horizon.api.batch;

import org.horizon.Cache;

import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractBatchTest {
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
