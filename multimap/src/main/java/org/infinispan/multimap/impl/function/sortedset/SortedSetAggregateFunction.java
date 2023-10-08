package org.infinispan.multimap.impl.function.sortedset;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.BaseSetBucket;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#union(Object, Collection, double, SortedSetBucket.AggregateFunction)}
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_AGGREGATE_FUNCTION)
public final class SortedSetAggregateFunction<K, V> implements SerializableFunction<EntryView.ReadWriteEntryView<K, ? extends BaseSetBucket<V>>, Collection<ScoredValue<V>>> {

   @ProtoField(1)
   final AggregateType type;

   @ProtoField(2)
   final Collection<ScoredValue<V>> scoredValues;

   @ProtoField(3)
   final double weight;

   @ProtoField(4)
   final SortedSetBucket.AggregateFunction function;

   @Proto
   @ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_AGGREGATE_FUNCTION_TYPE)
   public enum AggregateType {
      UNION, INTER;
   }

   @ProtoFactory
   public SortedSetAggregateFunction(AggregateType type,
                                     Collection<ScoredValue<V>> scoredValues,
                                     double weight,
                                     SortedSetBucket.AggregateFunction function) {
      this.type = type;
      this.scoredValues = scoredValues;
      this.weight = weight;
      this.function = function;
   }

   @Override
   public Collection<ScoredValue<V>> apply(EntryView.ReadWriteEntryView<K, ? extends BaseSetBucket<V>> view) {
      Optional<? extends BaseSetBucket<V>> existing = view.peek();
      if (scoredValues != null && scoredValues.isEmpty() && existing.isEmpty()) {
         return Collections.emptyList();
      }
      BaseSetBucket<V> bucket = existing.map(v -> (BaseSetBucket<V>) v).orElseGet(SortedSetBucket::new);
      return type == AggregateType.UNION
            ? bucket.union(scoredValues, weight, function)
            : bucket.inter(scoredValues, weight, function);
   }
}
