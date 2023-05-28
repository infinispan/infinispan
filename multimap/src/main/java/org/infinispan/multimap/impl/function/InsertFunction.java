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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#insert(Object, boolean, Object, Object)}
 * to insert an element before or after the provided pivot element.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class InsertFunction<K, V> implements ListBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<InsertFunction> EXTERNALIZER = new InsertFunction.Externalizer();
   private final boolean before;
   private final V pivot;
   private final V element;

   public InsertFunction(boolean before, V pivot, V element) {
      this.before = before;
      this.pivot = pivot;
      this.element = element;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket<V> bucket = existing.get().insert(before, pivot, element);
         if (bucket == null) {
            // pivot not found
            return -1L;
         } else {
            entryView.set(bucket);
            return (long) bucket.size();
         }

      }
      // key does not exist
      return 0L;
   }

   private static class Externalizer implements AdvancedExternalizer<InsertFunction> {

      @Override
      public Set<Class<? extends InsertFunction>> getTypeClasses() {
         return Collections.singleton(InsertFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INSERT_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, InsertFunction object) throws IOException {
         output.writeBoolean(object.before);
         output.writeObject(object.pivot);
         output.writeObject(object.element);
      }

      @Override
      public InsertFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException{
         return new InsertFunction(input.readBoolean(), input.readObject(), input.readObject());
      }
   }
}
