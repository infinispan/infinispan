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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#addMany(Object, double[], Object[], SortedSetAddArgs)}.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class AddManyFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<AddManyFunction> EXTERNALIZER = new Externalizer();
   private final double[] scores;
   private final V[] values;
   private final boolean addOnly;
   private final boolean updateOnly;
   private final boolean updateLessScoresOnly;
   private final boolean updateGreaterScoresOnly;
   private final boolean returnChangedCount;

   public AddManyFunction(double[] scores,
                          V[] values,
                          boolean addOnly,
                          boolean updateOnly,
                          boolean updateLessScoresOnly,
                          boolean updateGreaterScoresOnly,
                          boolean returnChangedCount) {
      this.scores = scores;
      this.values = values;
      this.addOnly = addOnly;
      this.updateOnly = updateOnly;
      this.updateLessScoresOnly = updateLessScoresOnly;
      this.updateGreaterScoresOnly = updateGreaterScoresOnly;
      this.returnChangedCount = returnChangedCount;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      SortedSetBucket bucket = null;
      if (existing.isPresent()) {
        bucket = existing.get();
      } else if (!updateOnly){
         bucket = new SortedSetBucket();
      }

      if (bucket != null) {
         SortedSetBucket.AddResult addResult = bucket.addMany(scores, values, addOnly, updateOnly, updateLessScoresOnly, updateGreaterScoresOnly);
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
         return ExternalizerIds.SORTED_SET_ADDMANY_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, AddManyFunction object) throws IOException {
         output.writeObject(object.scores);
         output.writeObject(object.values);
         output.writeBoolean(object.addOnly);
         output.writeBoolean(object.updateOnly);
         output.writeBoolean(object.updateLessScoresOnly);
         output.writeBoolean(object.updateGreaterScoresOnly);
         output.writeBoolean(object.returnChangedCount);
      }

      @Override
      public AddManyFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new AddManyFunction((double[]) input.readObject(), (Object[])input.readObject(), input.readBoolean(), input.readBoolean(), input.readBoolean(), input.readBoolean(), input.readBoolean());
      }
   }
}
