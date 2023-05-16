package org.infinispan.multimap.impl.function;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.ListBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#index(Object, long)}
 * to retrieve the index at the head or the tail of the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class IndexFunction<K, V> implements ListBucketBaseFunction<K, V, V> {
   public static final AdvancedExternalizer<IndexFunction> EXTERNALIZER = new IndexFunction.Externalizer();
   private final long index;

   public IndexFunction(long index) {
      this.index = index;
   }

   @Override
   public V apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().index(index);
      }
      // key does not exist
      return null;
   }

   private static class Externalizer implements AdvancedExternalizer<IndexFunction> {

      @Override
      public Set<Class<? extends IndexFunction>> getTypeClasses() {
         return Collections.singleton(IndexFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEX_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, IndexFunction object) throws IOException {
         output.writeLong(object.index);
      }

      @Override
      public IndexFunction readObject(ObjectInput input) throws IOException {
         return new IndexFunction(input.readLong());
      }
   }
}
