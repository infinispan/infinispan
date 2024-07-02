package org.infinispan.embedded;

import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncWeakCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * @since 15.0
 */
public class EmbeddedSyncWeakCounter implements SyncWeakCounter {
   private final Embedded embedded;
   private final WeakCounter counter;

   EmbeddedSyncWeakCounter(Embedded embedded, WeakCounter counter) {
      this.embedded = embedded;
      this.counter = counter;
   }

   @Override
   public String name() {
      return counter.getName();
   }

   @Override
   public SyncContainer container() {
      return embedded.sync();
   }

   @Override
   public long value() {
      return counter.getValue();
   }

   @Override
   public void add(long delta) {
      counter.add(delta);
   }
}
