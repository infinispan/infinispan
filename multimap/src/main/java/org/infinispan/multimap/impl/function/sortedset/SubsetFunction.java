package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.multimap.impl.SortedSetSubsetArgs;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#subsetByIndex(Object, SortedSetSubsetArgs)}
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#subsetByScore(Object, SortedSetSubsetArgs)}
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#subsetByLex(Object, SortedSetSubsetArgs)}
 *
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public class SubsetFunction<K, V, T> implements SortedSetBucketBaseFunction<K, V, Collection<SortedSetBucket.ScoredValue<V>>> {
   public static final AdvancedExternalizer<SubsetFunction> EXTERNALIZER = new Externalizer();
   protected final boolean isRev;
   protected final T start;
   protected final T stop;
   protected final boolean includeStart;
   protected final boolean includeStop;
   protected final Long offset;
   protected final Long count;
   protected final SortedSetOperationType subsetType;

   public SubsetFunction(SortedSetSubsetArgs<T> args, SortedSetOperationType subsetType) {
      this.isRev = args.isRev();
      this.start = args.getStart();
      this.stop = args.getStop();
      this.includeStart = args.isIncludeStart();
      this.includeStop = args.isIncludeStop();
      this.subsetType = subsetType;
      this.offset = args.getOffset();
      this.count = args.getCount();
   }

   public SubsetFunction(boolean isRev, T start, T stop, boolean includeStart, boolean includeStop,
                         Long offset, Long count, SortedSetOperationType subsetType) {
      this.isRev = isRev;
      this.start = start;
      this.stop = stop;
      this.includeStart = includeStart;
      this.includeStop = includeStop;
      this.offset = offset;
      this.count = count;
      this.subsetType = subsetType;
   }

   @Override
   public Collection<SortedSetBucket.ScoredValue<V>> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         SortedSetBucket<V> sortedSetBucket = existing.get();
         switch (subsetType) {
            case LEX:
               return sortedSetBucket.subset((V) start, includeStart, (V) stop, includeStop, isRev, offset, count);
            case SCORE:
               return sortedSetBucket.subset((Double) start, includeStart, (Double) stop, includeStop, isRev, offset, count);
            default:
               return sortedSetBucket.subsetByIndex((long) start, (long) stop, isRev);
         }

      }
      return Collections.emptySortedSet();
   }

   private static class Externalizer implements AdvancedExternalizer<SubsetFunction> {

      @Override
      public Set<Class<? extends SubsetFunction>> getTypeClasses() {
         return Set.of(SubsetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_SUBSET_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SubsetFunction object) throws IOException {
         output.writeBoolean(object.isRev);
         output.writeObject(object.start);
         output.writeObject(object.stop);
         output.writeBoolean(object.includeStart);
         output.writeBoolean(object.includeStop);
         output.writeObject(object.offset);
         output.writeObject(object.count);
         MarshallUtil.marshallEnum(object.subsetType, output);
      }

      @Override
      public SubsetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SubsetFunction(input.readBoolean(), input.readObject(), input.readObject(), input.readBoolean(),
               input.readBoolean(), (Long) input.readObject(), (Long) input.readObject(), MarshallUtil.unmarshallEnum(input, SortedSetOperationType::valueOf));
      }
   }
}
