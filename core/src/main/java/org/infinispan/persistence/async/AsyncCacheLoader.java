package org.infinispan.persistence.async;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.Store;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.support.DelegatingCacheLoader;

import net.jcip.annotations.GuardedBy;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class AsyncCacheLoader<K, V> extends DelegatingCacheLoader<K, V> {

   @GuardedBy("stateLock")
   protected final AtomicReference<State> state;

   public AsyncCacheLoader(CacheLoader actual, AtomicReference<State> state) {
      super(actual);
      this.state = state;
   }

   @Override
   public void start() {}

   @Override
   public void stop() {}

   @Override
   public MarshallableEntry<K, V> loadEntry(Object key) {
      Modification mod = state.get().get(key);
      if (mod != null) {
         switch (mod.getType()) {
            case REMOVE:
            case CLEAR:
               return null;
            case STORE:
               return ((Store) mod).getStoredValue();
         }
      }
      return super.loadEntry(key);
   }


   @Override
   public boolean contains(Object key) {
      Modification mod = state.get().get(key);
      if (mod != null)
         return mod.getType() == Modification.Type.STORE;

      return super.contains(key);
   }

}
