package org.infinispan.multimap.impl.function;

import org.infinispan.commons.CacheException;
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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#set(Object, long, Object)}
 * to insert a key/value pair at the index of the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class SetFunction<K, V> implements ListBucketBaseFunction<K, V, Boolean> {
   public static final AdvancedExternalizer<SetFunction> EXTERNALIZER = new Externalizer();
   private final long index;
   private final V value;

   public SetFunction(long index, V value) {
      this.index = index;
      this.value = value;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket<V> newBucket = existing.get().set(index, value);
         if (newBucket != null) {
            entryView.set(newBucket);
            return true;
         }

         throw new CacheException(new IndexOutOfBoundsException("Index is out of range"));
      }
      return false;
   }

   private static class Externalizer implements AdvancedExternalizer<SetFunction> {

      @Override
      public Set<Class<? extends SetFunction>> getTypeClasses() {
         return Collections.singleton(SetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SET_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SetFunction object) throws IOException {
         output.writeLong(object.index);
         output.writeObject(object.value);
      }

      @Override
      public SetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SetFunction(input.readLong(), input.readObject());
      }
   }
}
