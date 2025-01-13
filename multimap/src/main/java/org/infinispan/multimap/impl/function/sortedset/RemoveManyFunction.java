package org.infinispan.multimap.impl.function.sortedset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#removeAll(Object, Object, Object)}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_REMOVE_MANY_FUNCTION)
public final class RemoveManyFunction<K, V, T> implements SortedSetBucketBaseFunction<K, V, Long> {

   @ProtoField(value = 1, defaultValue = "false")
   final boolean includeMin;

   @ProtoField(value = 2, defaultValue = "false")
   final boolean includeMax;

   @ProtoField(3)
   final SortedSetOperationType type;
   private final List<T> values;

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

   @ProtoFactory
   RemoveManyFunction(boolean includeMin, boolean includeMax, SortedSetOperationType type, MarshallableCollection<T> values) {
      this(MarshallableCollection.unwrap(values, ArrayList::new), includeMin, includeMax, type);
   }

   @ProtoField(4)
   MarshallableCollection<T> getValues() {
      return MarshallableCollection.create(values);
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         SortedSetBucket<V> bucket = existing.get();
         var result = switch (type) {
            case LEX -> {
               V first = element(values, 0);
               yield bucket.removeAll(first, includeMin, element(values, 1), includeMax);
            }
            case SCORE -> {
               Double d = element(values, 0);
               yield bucket.removeAll(d, includeMin, element(values, 1), includeMax);
            }
            case INDEX -> {
               Long l = element(values, 0);
               yield bucket.removeAll(l, element(values, 1));
            }
            default -> bucket.removeAll(unchecked(values));
         };

         SortedSetBucket<V> next = result.bucket();
         if (next.size() == 0) {
            entryView.remove();
         } else {
            entryView.set(next);
         }
         return result.result();
      }
      // nothing has been done
      return 0L;
   }

   @SuppressWarnings("unchecked")
   private static <E> Collection<E> unchecked(List<?> list) {
      return (Collection<E>) list;
   }

   @SuppressWarnings("unchecked")
   private static <E> E element(List<?> list, int index) {
      return (E) list.get(index);
   }
}
