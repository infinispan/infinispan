package org.infinispan.multimap.impl.function.set;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
public final class SMIsMember<K, V> implements SetBucketBaseFunction<K, V, List<Long>> {
   public static final AdvancedExternalizer<SMIsMember> EXTERNALIZER = new Externalizer();
   private final V[] values;

   public SMIsMember(V... values) {
      this.values = values;
   }

   @Override
   public List<Long> apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      var result = new ArrayList<Long>();
      Optional<SetBucket<V>> existing = entryView.peek();
      var s = existing.isPresent() ? existing.get() : new SetBucket<V>();
      for (var v : values) {
         result.add(s.contains(v) ? 1L : 0L);
      }
      return result;
   }

   private static class Externalizer implements AdvancedExternalizer<SMIsMember> {

      @Override
      public Set<Class<? extends SMIsMember>> getTypeClasses() {
         return Collections.singleton(SMIsMember.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SET_MISMEMBER_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SMIsMember object) throws IOException {
         output.writeInt(object.values.length);
         for (var el : object.values) {
            output.writeObject(el);
         }
      }

      @Override
      public SMIsMember readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int size = input.readInt();
         Object[] values = new Object[size];
         for (int i = 0; i < size; i++) {
            values[i] = input.readObject();
         }
         return new SMIsMember<>(values);
      }
   }
}
