package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetAddArgs;
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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#addMany(Object, Collection, SortedSetAddArgs)}  .
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class AddManyFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<AddManyFunction> EXTERNALIZER = new Externalizer();
   private final Collection<SortedSetBucket.ScoredValue<V>> scoredValues;
   private final boolean addOnly;
   private final boolean updateOnly;
   private final boolean updateLessScoresOnly;
   private final boolean updateGreaterScoresOnly;
   private final boolean returnChangedCount;
   private final boolean replace;

   public AddManyFunction(Collection<SortedSetBucket.ScoredValue<V>> scoredValues, SortedSetAddArgs args) {
      this.scoredValues = scoredValues;
      this.addOnly = args.addOnly;
      this.updateOnly = args.updateOnly;
      this.updateLessScoresOnly = args.updateLessScoresOnly;
      this.updateGreaterScoresOnly = args.updateGreaterScoresOnly;
      this.returnChangedCount = args.returnChangedCount;
      this.replace = args.replace;
   }

   public AddManyFunction(Collection<SortedSetBucket.ScoredValue<V>> scoredValues,
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

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      SortedSetBucket bucket = null;
      if (existing.isPresent()) {
        bucket = existing.get();
      } else if (!updateOnly || !replace){
         bucket = new SortedSetBucket();
      }

      if (bucket != null) {
         if (replace) {
            bucket.replace(scoredValues);
            if (bucket.size() == 0) {
               entryView.remove();
            } else {
               entryView.set(bucket);
            }
            return bucket.size();
         }

         SortedSetBucket.AddOrUpdatesCounters addResult = bucket.addMany(scoredValues, addOnly, updateOnly, updateLessScoresOnly, updateGreaterScoresOnly);
         //don't change if nothing was added or updated. it avoids replicating a no-op
         if (addResult.updated > 0 || addResult.created > 0) {
            entryView.set(bucket);
         }
         // Return created only or created and updated count
         return returnChangedCount? addResult.created + addResult.updated : addResult.created;
      }

      // nothing has been done
      return 0L;

   }

   private static class Externalizer implements AdvancedExternalizer<AddManyFunction> {

      @Override
      public Set<Class<? extends AddManyFunction>> getTypeClasses() {
         return Collections.singleton(AddManyFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_ADD_MANY_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, AddManyFunction object) throws IOException {
         MarshallUtil.marshallCollection(object.scoredValues, output);
         output.writeBoolean(object.addOnly);
         output.writeBoolean(object.updateOnly);
         output.writeBoolean(object.updateLessScoresOnly);
         output.writeBoolean(object.updateGreaterScoresOnly);
         output.writeBoolean(object.returnChangedCount);
         output.writeBoolean(object.replace);
      }

      @Override
      public AddManyFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Collection<SortedSetBucket.ScoredValue> scoredValues = unmarshallCollection(input, ArrayList::new);
         return new AddManyFunction(scoredValues, input.readBoolean(), input.readBoolean(),
               input.readBoolean(), input.readBoolean(), input.readBoolean(), input.readBoolean());
      }
   }
}
