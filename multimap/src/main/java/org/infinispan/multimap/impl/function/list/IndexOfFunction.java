package org.infinispan.multimap.impl.function.list;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.ListBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#indexOf(Object, Object, Long, Long, Long)}
 * to retrieve the index at the head or the tail of the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class IndexOfFunction<K, V> implements ListBucketBaseFunction<K, V, Collection<Long>> {
   public static final AdvancedExternalizer<IndexOfFunction> EXTERNALIZER = new IndexOfFunction.Externalizer();
   private final V element;
   private final long count;
   private final long rank;
   private final long maxLen;

   public IndexOfFunction(V element, long count, long rank, long maxLen) {
      this.element = element;
      this.count = count;
      this.rank = rank;
      this.maxLen = maxLen;
   }

   @Override
   public Collection<Long> apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().indexOf(element, count, rank, maxLen);
      }
      // key does not exist
      return null;
   }

   private static class Externalizer implements AdvancedExternalizer<IndexOfFunction> {

      @Override
      public Set<Class<? extends IndexOfFunction>> getTypeClasses() {
         return Collections.singleton(IndexOfFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEXOF_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, IndexOfFunction object) throws IOException {
         output.writeObject(object.element);
         output.writeLong(object.count);
         output.writeLong(object.rank);
         output.writeLong(object.maxLen);
      }

      @Override
      public IndexOfFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException{
         return new IndexOfFunction(input.readObject(), input.readLong(), input.readLong(), input.readLong());
      }
   }
}
