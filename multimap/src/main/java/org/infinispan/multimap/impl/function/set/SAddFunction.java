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
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#add}
 * to add elements to a Set.
 *
 * @author Vittorio Rigamonti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of
 *      Functions</a>
 * @since 15.0
 */
public final class SAddFunction<K, V> implements SetBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<SAddFunction> EXTERNALIZER = new Externalizer();
   private final Collection<V> values;

   public SAddFunction(Collection<V> values) {
      this.values = values;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      Optional<SetBucket<V>> existing = entryView.peek();
      long added = 0L;
      var s = existing.orElseGet(SetBucket::new);
      var initSize = s.size();
      var res = s.addAll(values);
      s = res.bucket();
      if (res.result()) {
         added = s.size() - initSize;
      }
      // don't change the cache if the value already exists. it avoids replicating a
      // no-op
      if (added > 0) {
         entryView.set(s);
      }
      return added;
   }

   public Collection<V> values() {
      return values;
   }

   private static class Externalizer implements AdvancedExternalizer<SAddFunction> {

      @Override
      public Set<Class<? extends SAddFunction>> getTypeClasses() {
         return Collections.singleton(SAddFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SET_ADD_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SAddFunction object) throws IOException {
         output.writeInt(object.values().size());
         for (Object v : object.values()) {
            output.writeObject(v);
         }
      }

      @Override
      public SAddFunction<?, ?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         var size = input.readInt();
         var list = new ArrayList<>(size);
         for (int i = 0; i < size; i++) {
            list.add(input.readObject());
         }
         return new SAddFunction<>(list);
      }
   }

}
