package org.infinispan.persistence.async;

import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.CacheWriter;

import java.util.concurrent.Executor;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class AdvancedAsyncCacheWriter extends AsyncCacheWriter implements AdvancedCacheWriter {

   public AdvancedAsyncCacheWriter(CacheWriter delegate) {
      super(delegate);
   }

   @Override
   public void purge(Executor threadPool, PurgeListener task) {
      advancedWriter().purge(threadPool, task);
   }

   @Override
   public void clear() {
      stateLock.writeLock(1);
      try {
         state.set(newState(true, state.get().next));
      } finally {
         stateLock.reset(1);
         stateLock.writeUnlock();
      }
   }

   @Override
   protected void clearStore() {
      advancedWriter().clear();
   }

   private AdvancedCacheWriter advancedWriter() {
      return (AdvancedCacheWriter) actual;
   }
}
