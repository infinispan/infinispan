package org.infinispan.multimap.impl.function.sortedset;

import java.util.Collection;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#addMany(Object, Collection, SortedSetAddArgs)}  .
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_ADD_MANY_FUNCTION)
public final class AddManyFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Long> {

   @ProtoField(1)
   final boolean addOnly;

   @ProtoField(2)
   final boolean updateOnly;

   @ProtoField(3)
   final boolean updateLessScoresOnly;

   @ProtoField(4)
   final boolean updateGreaterScoresOnly;

   @ProtoField(5)
   final boolean returnChangedCount;

   @ProtoField(6)
   final boolean replace;

   private final Collection<ScoredValue<V>> scoredValues;

   public AddManyFunction(Collection<ScoredValue<V>> scoredValues, SortedSetAddArgs args) {
      this.scoredValues = scoredValues;
      this.addOnly = args.addOnly;
      this.updateOnly = args.updateOnly;
      this.updateLessScoresOnly = args.updateLessScoresOnly;
      this.updateGreaterScoresOnly = args.updateGreaterScoresOnly;
      this.returnChangedCount = args.returnChangedCount;
      this.replace = args.replace;
   }

   public AddManyFunction(Collection<ScoredValue<V>> scoredValues,
                          boolean addOnly, boolean updateOnly,
                          boolean updateLessScoresOnly,
                          boolean updateGreaterScoresOnly,
                          boolean returnChangedCount,
                          boolean replace) {
      this.scoredValues = scoredValues;
      this.addOnly = addOnly;
      this.updateOnly = updateOnly;
      this.updateLessScoresOnly = updateLessScoresOnly;
      this.updateGreaterScoresOnly = updateGreaterScoresOnly;
      this.returnChangedCount = returnChangedCount;
      this.replace = replace;
   }

   @ProtoFactory
   AddManyFunction(boolean addOnly, boolean updateOnly, boolean updateLessScoresOnly, boolean updateGreaterScoresOnly,
                   boolean returnChangedCount, boolean replace, Collection<ScoredValue<V>> scoredValues) {
      this(scoredValues, addOnly, updateOnly, updateLessScoresOnly, updateGreaterScoresOnly,
            returnChangedCount, replace);
   }

   @ProtoField(7)
   Collection<ScoredValue<V>> getScoredValues() {
      return scoredValues;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      SortedSetBucket<V> bucket = null;
      if (existing.isPresent()) {
        bucket = existing.get();
      } else if (!updateOnly || !replace){
         bucket = new SortedSetBucket<>();
      }

      if (bucket != null) {
         if (replace) {
            SortedSetBucket<V> next = bucket.replace(scoredValues);
            if (next.size() == 0) {
               entryView.remove();
            } else {
               entryView.set(next);
            }
            return next.size();
         }

         var res = bucket.addMany(scoredValues, addOnly, updateOnly, updateLessScoresOnly, updateGreaterScoresOnly);
         SortedSetBucket.AddOrUpdatesCounters addResult = res.result();
         //don't change if nothing was added or updated. it avoids replicating a no-op
         if (addResult.updated > 0 || addResult.created > 0) {
            entryView.set(res.bucket());
         }
         // Return created only or created and updated count
         return returnChangedCount? addResult.created + addResult.updated : addResult.created;
      }

      // nothing has been done
      return 0L;

   }
}
