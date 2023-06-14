package org.infinispan.multimap.impl.function.set;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SetBucket;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#get}
 * to get a Set value with the given key.
 *
 * @author Vittorio Rigamonti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class SGetFunction<K, V> implements SetBucketBaseFunction<K, V, SetBucket<V>> {

   public static final AdvancedExternalizer<SGetFunction> EXTERNALIZER = new Externalizer();

   @Override
   public SetBucket<V> apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      Optional<SetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get();
      }
      return new SetBucket<V>();
   }

   private static class Externalizer implements AdvancedExternalizer<SGetFunction> {
      @Override
      public Set<Class<? extends SGetFunction>> getTypeClasses() {
         return Collections.singleton(SGetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SET_GET_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SGetFunction object) throws IOException {
      }

      @Override
      public SGetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SGetFunction();
      }
   }
}
