package org.infinispan.multimap.impl.function.sortedset;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#incrementScore(Object, double, Object, SortedSetAddArgs)}   .
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_INCR_FUNCTION)
public final class IncrFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Double> {

   @ProtoField(1)
   final Double score;

   @ProtoField(2)
   final boolean addOnly;

   @ProtoField(3)
   final boolean updateOnly;

   @ProtoField(4)
   final boolean updateLessScoresOnly;

   @ProtoField(5)
   final boolean updateGreaterScoresOnly;

   private final V member;

   public IncrFunction(double score, V member, SortedSetAddArgs args) {
      this.score = score;
      this.member = member;
      this.addOnly = args.addOnly;
      this.updateOnly = args.updateOnly;
      this.updateLessScoresOnly = args.updateLessScoresOnly;
      this.updateGreaterScoresOnly = args.updateGreaterScoresOnly;
   }

   @ProtoFactory
   public IncrFunction(Double score, boolean addOnly, boolean updateOnly, boolean updateLessScoresOnly,
                       boolean updateGreaterScoresOnly, MarshallableObject<V> member) {
      this.score = score;
      this.addOnly = addOnly;
      this.updateOnly = updateOnly;
      this.updateLessScoresOnly = updateLessScoresOnly;
      this.updateGreaterScoresOnly = updateGreaterScoresOnly;
      this.member = MarshallableObject.unwrap(member);
   }

   @ProtoField(6)
   MarshallableObject<V> getMember() {
      return MarshallableObject.create(member);
   }

   @Override
   public Double apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      SortedSetBucket<V> bucket = null;
      if (existing.isPresent()) {
        bucket = existing.get();
      } else if (!updateOnly){
         bucket = new SortedSetBucket<>();
      }
      Double result = null;
      if (bucket != null) {
         var res = bucket.incrScore(score, member, addOnly, updateOnly, updateLessScoresOnly, updateGreaterScoresOnly);
         //don't change if nothing was added or updated. it avoids replicating a no-op
         if (res != null) {
            result = res.result();
            entryView.set(res.bucket());
         }
      }

      // Return member score or null of the incr function returns a null score
      return result;
   }
}
