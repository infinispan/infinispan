package org.infinispan.jcache.embedded.functions;

import java.util.Optional;
import java.util.function.Function;

import org.infinispan.functional.EntryView;

public class GetAndRemove<K, V> implements Function<EntryView.ReadWriteEntryView<K, V>, V> {
   private static final GetAndRemove INSTANCE = new GetAndRemove();

   @Override
   public V apply(EntryView.ReadWriteEntryView<K, V> view) {
      Optional<V> prev = view.find();
      view.set(null);
      return prev.orElse(null);
   }

   public static <K, V> GetAndRemove<K, V> getInstance() {
      return INSTANCE;
   }
}
