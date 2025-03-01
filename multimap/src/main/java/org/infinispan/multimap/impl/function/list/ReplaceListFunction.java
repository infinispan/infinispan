package org.infinispan.multimap.impl.function.list;

import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.ListBucket;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#replace(Object, List)}
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class ReplaceListFunction<K, V> implements ListBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<ReplaceListFunction> EXTERNALIZER = new Externalizer();
   private final Deque<V> values;

   public ReplaceListFunction(List<V> values) {
      this(ReplaceListFunction.get(values));
   }

   public ReplaceListFunction(Deque<V> values) {
      this.values = values;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      ListBucket<V> bucket = null;
      if (existing.isPresent()) {
        bucket = existing.get();
      } else if (values != null && !values.isEmpty()){
         bucket = new ListBucket<>();
      }

      if (bucket != null) {
         ListBucket<V> updated = bucket.replace(values);
         if (updated.isEmpty()) {
            entryView.remove();
         } else {
            entryView.set(updated);
         }
         return updated.size();
      }

      // nothing has been done
      return 0L;
   }

   @SuppressWarnings("unchecked")
   private static <E> Deque<E> get(List<E> values) {
      if (values == null || values instanceof Deque<?> dq)
         return (Deque<E>) values;

      return new ArrayDeque<>(values);
   }

   private static class Externalizer implements AdvancedExternalizer<ReplaceListFunction> {

      @Override
      public Set<Class<? extends ReplaceListFunction>> getTypeClasses() {
         return Collections.singleton(ReplaceListFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.REPLACE_LIST_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, ReplaceListFunction object) throws IOException {
         MarshallUtil.marshallCollection(object.values, output);
      }

      @Override
      public ReplaceListFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Deque values = unmarshallCollection(input, ArrayDeque::new);
         return new ReplaceListFunction(values);
      }
   }
}
