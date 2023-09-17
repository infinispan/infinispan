package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#count(K, double, boolean, double, boolean)}
 * and {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#count(Object, Object, boolean, Object, boolean)} .
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class CountFunction<K, V, T> implements SortedSetBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<CountFunction> EXTERNALIZER = new Externalizer();
   private final T min;
   private final T max;
   private final boolean includeMin;
   private final boolean includeMax;
   private final SortedSetOperationType countType;

   public CountFunction(T min,
                        boolean includeMin,
                        T max,
                        boolean includeMax,
                        SortedSetOperationType countType) {
      this.min = min;
      this.includeMin = includeMin;
      this.max = max;
      this.includeMax = includeMax;
      this.countType = countType;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      long count = 0;
      if (existing.isPresent()) {
         switch (countType) {
            case LEX:
               return (long) existing.get()
                     .subset((V) min, includeMin, (V) max, includeMax, false, null, null).size();
            case SCORE:
               return (long) existing.get()
                     .subset((Double) min, includeMin, (Double) max, includeMax, false, null, null).size();
         }
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
         output.writeObject(object.min);
         output.writeBoolean(object.includeMin);
         output.writeObject(object.max);
         output.writeBoolean(object.includeMax);
         MarshallUtil.marshallEnum(object.countType, output);
      }

      @Override
      public CountFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CountFunction(input.readObject(), input.readBoolean(), input.readObject(), input.readBoolean(),
               MarshallUtil.unmarshallEnum(input, SortedSetOperationType::valueOf));
      }
   }
}
