package org.infinispan.multimap.impl.function.set;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SetBucket;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#remove}
 * to remove elements to a Set.
 *
 * @author Vittorio Rigamonti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of
 *      Functions</a>
 * @since 15.0
 */
public final class SRemoveFunction<K, V> implements SetBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<SRemoveFunction> EXTERNALIZER = new Externalizer();
   private final Collection<V> values;

   public SRemoveFunction(Collection<V> values) {
      this.values = values;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      Optional<SetBucket<V>> existing = entryView.peek();
      Long removed = 0L;
      if (!existing.isPresent()) {
         return 0L;
      }
      var s = existing.get();
      var initSize = s.size();
      if (s.removeAll(values)) {
         removed = Long.valueOf(initSize - s.size());
      }
      // don't change the cache if the value already exists. it avoids replicating a
      // no-op
      if (removed > 0) {
         if (s.size() > 0) {
            entryView.set(s);
         } else {
            entryView.remove();
         }
      }
      return removed;
   }

   public Collection<V> values() {
      return values;
   }

   private static class Externalizer implements AdvancedExternalizer<SRemoveFunction> {

      @Override
      public Set<Class<? extends SRemoveFunction>> getTypeClasses() {
         return Collections.singleton(SRemoveFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SET_REMOVE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SRemoveFunction object) throws IOException {
         output.writeInt(object.values().size());
         for (Object v : object.values()) {
            output.writeObject(v);
         }
      }

      @Override
      public SRemoveFunction<?, ?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         var size = input.readInt();
         var list = new ArrayList<>(size);
         for (int i = 0; i < size; i++) {
            list.add(input.readObject());
         }
         return new SRemoveFunction<>(list);
      }
   }
}
