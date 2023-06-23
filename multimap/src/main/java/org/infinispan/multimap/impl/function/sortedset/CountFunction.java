package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#count(K, double, boolean, double, boolean)} .
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class CountFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<CountFunction> EXTERNALIZER = new Externalizer();
   private final double min;
   private final double max;
   private final boolean includeMin;
   private final boolean includeMax;

   public CountFunction(double min,
                        boolean includeMin,
                        double max,
                        boolean includeMax) {
      this.min = min;
      this.includeMin = includeMin;
      this.max = max;
      this.includeMax = includeMax;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      long count = 0;
      if (existing.isPresent()) {
         SortedSet<SortedSetBucket.ScoredValue<V>> scoredValues = existing.get()
               .subsetByScores(min, includeMin, max, includeMax);
         count = scoredValues.size();
      }
      return count;
   }

   private static class Externalizer implements AdvancedExternalizer<CountFunction> {

      @Override
      public Set<Class<? extends CountFunction>> getTypeClasses() {
         return Collections.singleton(CountFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_COUNT_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, CountFunction object) throws IOException {
         output.writeDouble(object.min);
         output.writeBoolean(object.includeMin);
         output.writeDouble(object.max);
         output.writeBoolean(object.includeMax);
      }

      @Override
      public CountFunction readObject(ObjectInput input) throws IOException {
         return new CountFunction(input.readDouble(), input.readBoolean(), input.readDouble(), input.readBoolean());
      }
   }
}
