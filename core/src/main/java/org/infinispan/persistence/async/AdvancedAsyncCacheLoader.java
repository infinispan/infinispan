package org.infinispan.persistence.async;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.infinispan.commons.util.ByRef;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.Store;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class AdvancedAsyncCacheLoader<K, V> extends AsyncCacheLoader<K, V> implements AdvancedCacheLoader<K, V> {

   private static final Log log = LogFactory.getLog(AdvancedAsyncCacheLoader.class);

   public AdvancedAsyncCacheLoader(CacheLoader actual, AtomicReference<State> state) {
      super(actual, state);
   }

   @Override
   public Publisher<K> publishKeys(Predicate<? super K> filter) {
      State state = this.state.get();
      ByRef<Boolean> hadClear = new ByRef<>(Boolean.FALSE);
      Map<Object, Modification> modificationMap = state.flattenModifications(hadClear);
      if (modificationMap.isEmpty()) {
         return advancedLoader().publishKeys(filter);
      }

      Flowable<K> modFlowable = Flowable.fromIterable(modificationMap.entrySet())
            // REMOVE we ignore, LIST and CLEAR aren't possible
            .filter(me -> Modification.Type.STORE == me.getValue().getType())
            .map(e -> (K) e.getKey());

      if (filter != null) {
         modFlowable = modFlowable.filter(filter::test);
      }

      if (hadClear.get() == Boolean.TRUE) {
         return modFlowable;
      }
      if (filter == null) {
         filter = k -> !modificationMap.containsKey(k);
      } else {
         filter = filter.and(k -> !modificationMap.containsKey(k));
      }
      return Flowable.merge(modFlowable, advancedLoader().publishKeys(filter));
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      State state = this.state.get();
      ByRef<Boolean> hadClear = new ByRef<>(Boolean.FALSE);
      Map<Object, Modification> modificationMap = state.flattenModifications(hadClear);
      if (modificationMap.isEmpty()) {
         return advancedLoader().publishEntries(filter, fetchValue, fetchMetadata);
      }
      Flowable<MarshalledEntry<K, V>> modFlowable = Flowable.fromIterable(modificationMap.entrySet())
            .map(Map.Entry::getValue)
            // REMOVE we ignore, LIST and CLEAR aren't possible
            .filter(e -> Modification.Type.STORE == e.getType())
            .cast(Store.class)
            .map(Store::getStoredValue);

      if (filter != null) {
         Predicate<? super K> modificationFilter = filter;
         modFlowable = modFlowable.filter(e -> modificationFilter.test(e.getKey()));
      }

      // If we encountered a clear just ignore the actual store
      if (hadClear.get() == Boolean.TRUE) {
         return modFlowable;
      }
      if (filter == null) {
         filter = k -> !modificationMap.containsKey(k);
      } else {
         // Only use entry if it wasn't in modification map and passes filter
         filter = filter.and(k -> !modificationMap.containsKey(k));
      }
      return Flowable.merge(modFlowable, advancedLoader().publishEntries(filter, fetchValue, fetchMetadata));
   }

   @Override
   public int size() {
      //an estimate value anyway
      return advancedLoader().size();
   }

   AdvancedCacheLoader<K, V> advancedLoader() {
      return (AdvancedCacheLoader) actual;
   }
}
