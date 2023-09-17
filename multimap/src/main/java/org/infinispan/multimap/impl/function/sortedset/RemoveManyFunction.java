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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#removeAll(Object, Object, Object)}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class RemoveManyFunction<K, V, T> implements SortedSetBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<RemoveManyFunction> EXTERNALIZER = new Externalizer();
   private final List<T> values;
   private final boolean includeMin;
   private final boolean includeMax;
   private final SortedSetOperationType type;

   public RemoveManyFunction(List<T> values, SortedSetOperationType type) {
      this.values = values;
      this.includeMin = false;
      this.includeMax = false;
      this.type = type;
   }

   public RemoveManyFunction(List<T> values, boolean includeMin, boolean includeMax, SortedSetOperationType type) {
      this.values = values;
      this.includeMin = includeMin;
      this.includeMax = includeMax;
      this.type = type;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         SortedSetBucket bucket = existing.get();
         long removeCount;
         switch (type) {
            case LEX:
               removeCount = bucket.removeAll((V)values.get(0), includeMin, (V)values.get(1), includeMax);
               break;
            case SCORE:
               removeCount = bucket.removeAll((Double)values.get(0), includeMin, (Double)values.get(1), includeMax);
               break;
            case INDEX:
               removeCount =  bucket.removeAll((Long)values.get(0), (Long)values.get(1));
               break;
            default:
               removeCount = bucket.removeAll(values);
         }

         if (bucket.size() == 0) {
            entryView.remove();
         } else {
            entryView.set(bucket);
         }
         return removeCount;
      }
      // nothing has been done
      return 0L;
   }

   private static class Externalizer implements AdvancedExternalizer<RemoveManyFunction> {

      @Override
      public Set<Class<? extends RemoveManyFunction>> getTypeClasses() {
         return Collections.singleton(RemoveManyFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_REMOVE_MANY_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, RemoveManyFunction object) throws IOException {
         MarshallUtil.marshallCollection(object.values, output);
         output.writeBoolean(object.includeMin);
         output.writeBoolean(object.includeMax);
         MarshallUtil.marshallEnum(object.type, output);
      }

      @Override
      public RemoveManyFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new RemoveManyFunction(unmarshallCollection(input, ArrayList::new),
               input.readBoolean(), input.readBoolean(),
               MarshallUtil.unmarshallEnum(input, SortedSetOperationType::valueOf));
      }
   }
}
