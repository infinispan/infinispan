package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#pop(Object, double, long)}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class PopFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Collection<SortedSetBucket.ScoredValue<V>>> {
   public static final AdvancedExternalizer<PopFunction> EXTERNALIZER = new Externalizer();
   private final boolean min;
   private final long count;

   public PopFunction(boolean min, long count) {
      this.min = min;
      this.count = count;
   }

   @Override
   public Collection<SortedSetBucket.ScoredValue<V>> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         SortedSetBucket<V> sortedSetBucket = existing.get();
         Collection<SortedSetBucket.ScoredValue<V>> poppedValues = sortedSetBucket.pop(min, count);
         if (sortedSetBucket.size() == 0) {
            entryView.remove();
         }
         return poppedValues;
      }
      return Collections.emptyList();
   }

   private static class Externalizer implements AdvancedExternalizer<PopFunction> {

      @Override
      public Set<Class<? extends PopFunction>> getTypeClasses() {
         return Collections.singleton(PopFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_POP_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, PopFunction object) throws IOException {
         output.writeBoolean(object.min);
         output.writeLong(object.count);
      }

      @Override
      public PopFunction readObject(ObjectInput input) throws IOException {
         return new PopFunction(input.readBoolean(), input.readLong());
      }
   }
}
