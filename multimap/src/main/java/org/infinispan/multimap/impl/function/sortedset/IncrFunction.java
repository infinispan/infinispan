package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#incrementScore(Object, double, Object, SortedSetAddArgs)}   .
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class IncrFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Double> {
   public static final AdvancedExternalizer<IncrFunction> EXTERNALIZER = new Externalizer();
   private final Double score;
   private final V member;
   private final boolean addOnly;
   private final boolean updateOnly;
   private final boolean updateLessScoresOnly;
   private final boolean updateGreaterScoresOnly;

   public IncrFunction(double score, V member, SortedSetAddArgs args) {
      this.score = score;
      this.member = member;
      this.addOnly = args.addOnly;
      this.updateOnly = args.updateOnly;
      this.updateLessScoresOnly = args.updateLessScoresOnly;
      this.updateGreaterScoresOnly = args.updateGreaterScoresOnly;
   }

   public IncrFunction(double score, V value,
                       boolean addOnly,
                       boolean updateOnly,
                       boolean updateLessScoresOnly,
                       boolean updateGreaterScoresOnly) {
      this.score = score;
      this.member = value;
      this.addOnly = addOnly;
      this.updateOnly = updateOnly;
      this.updateLessScoresOnly = updateLessScoresOnly;
      this.updateGreaterScoresOnly = updateGreaterScoresOnly;
   }

   @Override
   public Double apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      SortedSetBucket bucket = null;
      if (existing.isPresent()) {
        bucket = existing.get();
      } else if (!updateOnly){
         bucket = new SortedSetBucket();
      }
      Double result = null;
      if (bucket != null) {
         result = bucket.incrScore(score, member, addOnly, updateOnly, updateLessScoresOnly, updateGreaterScoresOnly);
         //don't change if nothing was added or updated. it avoids replicating a no-op
         if (result != null) {
            entryView.set(bucket);
         }
      }

      // Return member score or null of the incr function returns a null score
      return result;
   }

   private static class Externalizer implements AdvancedExternalizer<IncrFunction> {

      @Override
      public Set<Class<? extends IncrFunction>> getTypeClasses() {
         return Collections.singleton(IncrFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_INCR_SCORE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, IncrFunction object) throws IOException {
         output.writeDouble(object.score);
         output.writeObject(object.member);
         output.writeBoolean(object.addOnly);
         output.writeBoolean(object.updateOnly);
         output.writeBoolean(object.updateLessScoresOnly);
         output.writeBoolean(object.updateGreaterScoresOnly);
      }

      @Override
      public IncrFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new IncrFunction(input.readDouble(), input.readObject(), input.readBoolean(), input.readBoolean(),
               input.readBoolean(), input.readBoolean());
      }
   }
}
