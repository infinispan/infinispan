package org.infinispan.multimap.impl.function.hmap;

import static org.infinispan.multimap.impl.ExternalizerIds.HASH_MAP_REMOVE_FUNCTION;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.HashMapBucket;

public class HashMapRemoveFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Integer> {
   public static final Externalizer EXTERNALIZER = new Externalizer();

   private final Collection<HK> keys;

   public HashMapRemoveFunction(Collection<HK> keys) {
      this.keys = keys;
   }

   @Override
   public Integer apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      int res = 0;
      Optional<HashMapBucket<HK, HV>> existing = view.peek();

      if (existing.isPresent()) {
         HashMapBucket<HK, HV> bucket = existing.get();
         res = bucket.removeAll(keys);

         if (bucket.isEmpty()) {
            view.remove();
         } else {
            view.set(bucket);
         }
      }
      return res;
   }

   @SuppressWarnings({"rawtypes", "deprecation"})
   private static class Externalizer implements AdvancedExternalizer<HashMapRemoveFunction> {

      @Override
      public Set<Class<? extends HashMapRemoveFunction>> getTypeClasses() {
         return Collections.singleton(HashMapRemoveFunction.class);
      }

      @Override
      public Integer getId() {
         return HASH_MAP_REMOVE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, HashMapRemoveFunction object) throws IOException {
         output.writeObject(object.keys);
      }

      @Override
      @SuppressWarnings("unchecked")
      public HashMapRemoveFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Collection keys = (Collection) input.readObject();
         return new HashMapRemoveFunction(keys);
      }
   }
}
