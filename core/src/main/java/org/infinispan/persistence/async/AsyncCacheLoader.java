package org.infinispan.persistence.async;

import net.jcip.annotations.GuardedBy;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.Store;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.support.DelegatingCacheLoader;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class AsyncCacheLoader extends DelegatingCacheLoader {

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
   public MarshalledEntry load(Object key) {
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
      return super.load(key);
   }


   @Override
   public boolean contains(Object key) {
      Modification mod = state.get().get(key);
      if (mod != null)
         return mod.getType() == Modification.Type.STORE;

      return super.contains(key);
   }

}
