package org.infinispan.multimap.impl.function.list;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.ListBucket;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#pollFirst(K, long)} and
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#pollLast(K, long)}
 * to poll N values at the head or the tail of the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class PollFunction<K, V> implements ListBucketBaseFunction<K, V, Collection<V>> {
   public static final AdvancedExternalizer<PollFunction> EXTERNALIZER = new PollFunction.Externalizer();
   private final boolean first;
   private final long count;

   public PollFunction(boolean first, long count) {
      this.first = first;
      this.count = count;
   }

   @Override
   public Collection<V> apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         if (count == 0) {
            // Do nothing and return an empty list
            return List.of();
         }

         ListBucket.ListBucketResult<Collection<V>, V> result = existing.get().poll(first, count);
         if (result.bucket().isEmpty()) {
            entryView.remove();
         } else {
            entryView.set(result.bucket());
         }
         return result.result();
      }
      // key does not exist
      return null;
   }

   private static class Externalizer implements AdvancedExternalizer<PollFunction> {

      @Override
      public Set<Class<? extends PollFunction>> getTypeClasses() {
         return Collections.singleton(PollFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.POLL_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, PollFunction object) throws IOException {
         output.writeBoolean(object.first);
         output.writeLong(object.count);
      }

      @Override
      public PollFunction readObject(ObjectInput input) throws IOException {
         return new PollFunction(input.readBoolean(), input.readLong());
      }
   }
}
