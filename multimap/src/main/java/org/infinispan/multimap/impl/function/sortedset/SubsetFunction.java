package org.infinispan.multimap.impl.function.sortedset;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.multimap.impl.SortedSetSubsetArgs;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

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
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SUBSET_FUNCTION)
public class SubsetFunction<K, V, T> implements SortedSetBucketBaseFunction<K, V, Collection<ScoredValue<V>>> {

   @ProtoField(value = 1, defaultValue = "false", name = "rev")
   final boolean isRev;

   @ProtoField(value = 2, defaultValue = "false")
   final boolean includeStart;

   @ProtoField(value = 3, defaultValue = "false")
   final boolean includeStop;

   @ProtoField(4)
   final Long offset;

   @ProtoField(5)
   final Long count;

   @ProtoField(6)
   final SortedSetOperationType subsetType;

   final T start;
   final T stop;

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

   @ProtoFactory
   SubsetFunction(boolean isRev, boolean includeStart, boolean includeStop, Long offset, Long count,
                  SortedSetOperationType subsetType, MarshallableObject<T> start, MarshallableObject<T> stop) {
      this.isRev = isRev;
      this.includeStart = includeStart;
      this.includeStop = includeStop;
      this.offset = offset;
      this.count = count;
      this.subsetType = subsetType;
      this.start = MarshallableObject.unwrap(start);
      this.stop = MarshallableObject.unwrap(stop);
   }

   @ProtoField(7)
   MarshallableObject<T> getStart() {
      return MarshallableObject.create(start);
   }

   @ProtoField(8)
   MarshallableObject<T> getStop() {
      return MarshallableObject.create(stop);
   }

   @Override
   public Collection<ScoredValue<V>> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
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
      return Collections.emptySet();
   }
}
