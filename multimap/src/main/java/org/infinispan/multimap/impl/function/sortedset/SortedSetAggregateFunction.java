package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#union(Object, Collection, double, SortedSetBucket.AggregateFunction)}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class SortedSetAggregateFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Collection<SortedSetBucket.ScoredValue<V>>> {
   public static final AdvancedExternalizer<SortedSetAggregateFunction> EXTERNALIZER = new Externalizer();
   private final Collection<SortedSetBucket.ScoredValue<V>> scoredValues;
   private final double weight;
   private final SortedSetBucket.AggregateFunction function;
   private final AggregateType type;

   public enum AggregateType {
      UNION, INTER;

      private static final SortedSetAggregateFunction.AggregateType[] CACHED_VALUES = values();
      public static SortedSetAggregateFunction.AggregateType valueOf(int ordinal) {
         return CACHED_VALUES[ordinal];
      }
   }

   public SortedSetAggregateFunction(AggregateType type,
                                     Collection<SortedSetBucket.ScoredValue<V>> scoredValues,
                                     double weight,
                                     SortedSetBucket.AggregateFunction function) {
      this.type = type;
      this.scoredValues = scoredValues;
      this.weight = weight;
      this.function = function;
   }

   @Override
   public Collection<SortedSetBucket.ScoredValue<V>> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (scoredValues != null && scoredValues.isEmpty() && existing.isEmpty()) {
         return Collections.emptyList();
      }
      SortedSetBucket<V> bucket = existing.isPresent() ? existing.get() : new SortedSetBucket();
      return type == AggregateType.UNION ? bucket.union(scoredValues, weight, function) : bucket.inter(scoredValues, weight, function);
   }

   private static class Externalizer implements AdvancedExternalizer<SortedSetAggregateFunction> {

      @Override
      public Set<Class<? extends SortedSetAggregateFunction>> getTypeClasses() {
         return Collections.singleton(SortedSetAggregateFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_AGGREGATE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SortedSetAggregateFunction object) throws IOException {
         MarshallUtil.marshallEnum(object.type, output);
         MarshallUtil.marshallCollection(object.scoredValues, output);
         output.writeDouble(object.weight);
         MarshallUtil.marshallEnum(object.function, output);
      }

      @Override
      public SortedSetAggregateFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         AggregateType aggregateType = MarshallUtil.unmarshallEnum(input, AggregateType::valueOf);
         Collection<SortedSetBucket.ScoredValue> scoredValues = unmarshallCollection(input, ArrayList::new);
         double weight = input.readDouble();
         SortedSetBucket.AggregateFunction aggregateFunction = MarshallUtil.unmarshallEnum(input, SortedSetBucket.AggregateFunction::valueOf);
         return new SortedSetAggregateFunction(aggregateType, scoredValues, weight, aggregateFunction);
      }
   }
}
