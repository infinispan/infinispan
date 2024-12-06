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
