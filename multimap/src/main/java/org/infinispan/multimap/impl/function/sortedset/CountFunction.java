package org.infinispan.multimap.impl.function.sortedset;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#count(K, double, boolean, double, boolean)}
 * and {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#count(Object, Object, boolean, Object, boolean)} .
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_COUNT_FUNCTION)
public final class CountFunction<K, V, T> implements SortedSetBucketBaseFunction<K, V, Long> {

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

   @ProtoFactory
   CountFunction(MarshallableObject<T> min, boolean includeMin, MarshallableObject<T> max, boolean includeMax, SortedSetOperationType countType) {
      this(MarshallableObject.unwrap(min), includeMin, MarshallableObject.unwrap(max), includeMax, countType);
   }

   @ProtoField(1)
   MarshallableObject<T> getMin() {
      return MarshallableObject.create(min);
   }

   @ProtoField(2)
   boolean isIncludeMin() {
      return includeMin;
   }

   @ProtoField(3)
   MarshallableObject<T> getMax() {
      return MarshallableObject.create(max);
   }

   @ProtoField(4)
   boolean isIncludeMax() {
      return includeMax;
   }

   @ProtoField(5)
   SortedSetOperationType getCountType() {
      return countType;
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
}
