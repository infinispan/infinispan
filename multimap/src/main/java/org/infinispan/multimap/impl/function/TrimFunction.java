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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#trim(Object, long, long)}
 * to trim the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class TrimFunction<K, V> implements ListBucketBaseFunction<K, V, Boolean> {
   public static final AdvancedExternalizer<TrimFunction> EXTERNALIZER = new TrimFunction.Externalizer();
   private final long from;
   private final long to;

   public TrimFunction(long from, long to) {
      this.from = from;
      this.to = to;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket bucket = existing.get();
         bucket.trim(from, to);
         if (bucket.isEmpty()) {
            entryView.remove();
         }
         return true;
      }
      // key does not exist
      return false;
   }

   private static class Externalizer implements AdvancedExternalizer<TrimFunction> {

      @Override
      public Set<Class<? extends TrimFunction>> getTypeClasses() {
         return Collections.singleton(TrimFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.TRIM_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, TrimFunction object) throws IOException {
         output.writeLong(object.from);
         output.writeLong(object.to);
      }

      @Override
      public TrimFunction readObject(ObjectInput input) throws IOException {
         return new TrimFunction(input.readLong(), input.readLong());
      }
   }
}
