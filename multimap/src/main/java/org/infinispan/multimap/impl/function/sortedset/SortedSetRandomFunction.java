package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#randomMembers(Object, int)} )}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class SortedSetRandomFunction<K, V> implements SortedSetBucketBaseFunction<K, V, List<SortedSetBucket.ScoredValue<V>>> {
   public static final AdvancedExternalizer<SortedSetRandomFunction> EXTERNALIZER = new Externalizer();
   private final int count;

   public SortedSetRandomFunction(int count) {
      this.count = count;
   }

   @Override
   public List<SortedSetBucket.ScoredValue<V>> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (count == 0 || existing.isEmpty()) {
         return Collections.emptyList();
      }
      return existing.get().randomMembers(count);
   }

   private static class Externalizer implements AdvancedExternalizer<SortedSetRandomFunction> {

      @Override
      public Set<Class<? extends SortedSetRandomFunction>> getTypeClasses() {
         return Collections.singleton(SortedSetRandomFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_RANDOM_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SortedSetRandomFunction object) throws IOException {
         output.writeInt(object.count);
      }

      @Override
      public SortedSetRandomFunction readObject(ObjectInput input) throws IOException {
         return new SortedSetRandomFunction(input.readInt());
      }
   }
}
