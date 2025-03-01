package org.infinispan.multimap.impl.function.list;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.ListBucket;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#rotate(Object, boolean)} (Object, boolean)}
 * to remove an element.
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class RotateFunction<K, V> implements ListBucketBaseFunction<K, V, V> {
   public static final AdvancedExternalizer<RotateFunction> EXTERNALIZER = new RotateFunction.Externalizer();
   private final boolean rotateRight;

   public RotateFunction(boolean rotateRight) {
      this.rotateRight = rotateRight;
   }

   @Override
   public V apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket.ListBucketResult<V, V> result = existing.get().rotate(rotateRight);
         entryView.set(result.bucket());
         return result.result();
      }
      // key does not exist
      return null;
   }

   private static class Externalizer implements AdvancedExternalizer<RotateFunction> {

      @Override
      public Set<Class<? extends RotateFunction>> getTypeClasses() {
         return Collections.singleton(RotateFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ROTATE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, RotateFunction object) throws IOException {
         output.writeBoolean(object.rotateRight);
      }

      @Override
      public RotateFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException{
         return new RotateFunction(input.readBoolean());
      }
   }
}
