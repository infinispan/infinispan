package org.infinispan.multimap.impl.function.list;

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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#remove(Object, long, Object)}
 * to remove an element.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class RemoveCountFunction<K, V> implements ListBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<RemoveCountFunction> EXTERNALIZER = new RemoveCountFunction.Externalizer();
   private final long count;
   private final V element;

   public RemoveCountFunction(long count, V element) {
      this.count = count;
      this.element = element;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket<V> prevBucket = existing.get();
         long removedCount = existing.get().remove(count, element);
         if (prevBucket.isEmpty()) {
            // if the list is empty, remove
            entryView.remove();
         }
         return removedCount;
      }
      // key does not exist
      return 0L;
   }

   private static class Externalizer implements AdvancedExternalizer<RemoveCountFunction> {

      @Override
      public Set<Class<? extends RemoveCountFunction>> getTypeClasses() {
         return Collections.singleton(RemoveCountFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.REMOVE_COUNT_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, RemoveCountFunction object) throws IOException {
         output.writeLong(object.count);
         output.writeObject(object.element);
      }

      @Override
      public RemoveCountFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException{
         return new RemoveCountFunction(input.readLong(), input.readObject());
      }
   }
}
