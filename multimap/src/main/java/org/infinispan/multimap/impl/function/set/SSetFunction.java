package org.infinispan.multimap.impl.function.set;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SetBucket;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#set}
 * to operator on Sets.
 *
 * @author Vittorio Rigamonti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of
 *      Functions</a>
 * @since 15.0
 */
public final class SSetFunction<K, V> implements SetBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<SSetFunction> EXTERNALIZER = new Externalizer();
   private final Collection<V> values;

   public SSetFunction(Collection<V> values) {
      this.values = values;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      var set = new SetBucket<V>(new HashSet<>(values));
      entryView.set(set);
      return (long) set.size();
   }

   private static class Externalizer implements AdvancedExternalizer<SSetFunction> {

      @Override
      public Set<Class<? extends SSetFunction>> getTypeClasses() {
         return Collections.singleton(SSetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SET_SET_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SSetFunction object) throws IOException {
         output.writeInt(object.values.size());
         for (var el : object.values) {
            output.writeObject(el);
         }
      }

      @Override
      public SSetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int size = input.readInt();
         Set<Object> values = new HashSet<>(size);
         for (int i = 0; i < size; i++) {
            values.add(input.readObject());
         }
         return new SSetFunction<>(values);
      }
   }
}
